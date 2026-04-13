import socket
import time
import random
from typing import Dict, List, Optional


class UdpControllerClient:
    def __init__(self, port: int = 3274, timeout_sec: float = 2.0) -> None:
        self.port = port
        self.timeout_sec = timeout_sec

    def _send_and_receive_once(self, payload: bytes, target_ip: str) -> Optional[str]:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            sock.settimeout(self.timeout_sec)
            sock.sendto(payload, (target_ip, self.port))
            data, _ = sock.recvfrom(512)
            return data.decode("utf-8", errors="ignore").strip()

    def discover_controller(self, broadcast_ip: str = "192.168.1.255") -> Optional[str]:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
            sock.settimeout(self.timeout_sec)
            sock.sendto(b"req", (broadcast_ip, self.port))
            try:
                data, addr = sock.recvfrom(512)
            except socket.timeout:
                return None

            text = data.decode("utf-8", errors="ignore").strip()
            if text == "ctrl":
                return addr[0]
            return None

    def request_version(self, controller_ip: str) -> Dict[str, Optional[str]]:
        text = self._send_and_receive_once(b"ver", controller_ip)
        if not text:
            return {"raw": None, "hw": None, "fw": None}

        parts = text.split(",")
        if len(parts) >= 2:
            return {"raw": text, "hw": parts[0].strip(), "fw": parts[1].strip()}
        return {"raw": text, "hw": None, "fw": None}

    def request_sensors(self, controller_ip: str, expected_sensors: int = 8) -> Dict[str, object]:
        values: List[int] = []
        raw_messages: List[str] = []
        frame_complete = False
        error_text = None
        t0 = time.time()

        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            sock.settimeout(self.timeout_sec)
            sock.sendto(b"sens", (controller_ip, self.port))

            while True:
                try:
                    data, _ = sock.recvfrom(512)
                except socket.timeout:
                    error_text = "timeout"
                    break

                text = data.decode("utf-8", errors="ignore").strip()
                raw_messages.append(text)

                if text == "ser":
                    error_text = "sensor_error_ser"
                    break

                try:
                    number = int(text)
                except ValueError:
                    # Ignore non-numeric packets observed occasionally in reverse-engineered protocol.
                    continue

                if number == 255:
                    frame_complete = True
                    break

                values.append(number)
                if len(values) >= expected_sensors:
                    # The original app expects at most 8 values per frame.
                    continue

        latency_ms = int((time.time() - t0) * 1000)
        while len(values) < expected_sensors:
            values.append(None)

        return {
            "values": values[:expected_sensors],
            "frame_complete": frame_complete,
            "raw_messages": raw_messages,
            "error_text": error_text,
            "latency_ms": latency_ms,
        }


class MockUdpControllerClient:
    def __init__(
        self,
        timeout_sec: float = 2.0,
        seed: Optional[int] = None,
        timeout_rate: float = 0.08,
        ser_rate: float = 0.05,
        scenario: str = "normal",
    ) -> None:
        self.timeout_sec = timeout_sec
        self.timeout_rate = timeout_rate
        self.ser_rate = ser_rate
        self.scenario = scenario
        self.random = random.Random(seed)
        self._base = [42, 44, 46, 45, 43, 47, 40, 41]
        self._cycle_index = 0
        self._burst_remaining = 0

    def discover_controller(self, broadcast_ip: str = "192.168.1.255") -> Optional[str]:
        _ = broadcast_ip
        return "192.168.1.50"

    def request_version(self, controller_ip: str) -> Dict[str, Optional[str]]:
        _ = controller_ip
        return {"raw": "HW-2.1,FW-3.4.7", "hw": "HW-2.1", "fw": "FW-3.4.7"}

    def request_sensors(self, controller_ip: str, expected_sensors: int = 8) -> Dict[str, object]:
        _ = controller_ip
        t0 = time.time()
        self._cycle_index += 1

        # Simula latenza LAN realistica.
        time.sleep(self.random.uniform(0.02, 0.12))

        timeout_prob = self.timeout_rate
        ser_prob = self.ser_rate

        if self.scenario == "burst_loss":
            if self._burst_remaining > 0:
                self._burst_remaining -= 1
                timeout_prob = 0.95
                ser_prob = 0.0
            elif self.random.random() < 0.08:
                self._burst_remaining = self.random.randint(2, 6)

        if self.random.random() < timeout_prob:
            latency_ms = int((time.time() - t0) * 1000)
            return {
                "values": [None] * expected_sensors,
                "frame_complete": False,
                "raw_messages": [],
                "error_text": "timeout",
                "latency_ms": latency_ms,
            }

        if self.random.random() < ser_prob:
            latency_ms = int((time.time() - t0) * 1000)
            return {
                "values": [None] * expected_sensors,
                "frame_complete": False,
                "raw_messages": ["ser"],
                "error_text": "sensor_error_ser",
                "latency_ms": latency_ms,
            }

        values: List[int] = []
        raw_messages: List[str] = []

        drift_offset = 0
        if self.scenario == "drift":
            drift_offset = min(15, self._cycle_index // 10)

        warmup_offset = 0
        if self.scenario == "warmup":
            warmup_offset = min(10, self._cycle_index // 5)

        spike_sensor = -1
        spike_delta = 0
        if self.scenario == "spike" and self.random.random() < 0.18:
            spike_sensor = self.random.randint(0, max(0, expected_sensors - 1))
            spike_delta = self.random.choice([12, 15, 18, -10, -12])

        for i in range(expected_sensors):
            baseline = self._base[i % len(self._base)]
            value = baseline + self.random.randint(-2, 2) + drift_offset + warmup_offset
            if i == spike_sensor:
                value += spike_delta
            values.append(value)
            raw_messages.append(str(value))

        raw_messages.append("255")
        latency_ms = int((time.time() - t0) * 1000)
        return {
            "values": values,
            "frame_complete": True,
            "raw_messages": raw_messages,
            "error_text": None,
            "latency_ms": latency_ms,
        }
