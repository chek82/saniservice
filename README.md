# app_discovery

Collector UDP in Python + Streamlit per discovery controller LAN e salvataggio dati sensori su SQLite.

## Funzioni

- Discovery controller via broadcast (`req` -> atteso `ctrl`)
- Lettura versione (`ver` -> `HW,FW`)
- Polling sensori (`sens` -> valori ASCII con terminatore `255`)
- Modalita mock per simulare ricezione UDP senza controller reale
- Salvataggio su DB SQLite
- Tabella e grafico in Streamlit
- Sezione report sanificazione con:
	- anagrafica intervento
	- dati termici da CSV o input manuale
	- dati termici da storico collector tramite dropdown codice attivita
	- verifica automatica soglia/tempo sopra soglia
	- export PDF finale

## Codice attivita e riuso misure

- In tab Collector, imposta `Codice attivita per salvataggio` prima del polling.
- Tutti i frame raccolti vengono associati a quel codice.
- In tab Report, scegli `Storico collector` e seleziona il codice attivita dal dropdown.
- Puoi scegliere come convertire i frame in curva termica: media sensori, massimo sensori, oppure singolo sensore `s1..s8`.

## Setup

```powershell
cd app_discovery
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
streamlit run app.py
```

## Note protocollo

- Porta UDP di default: `3274`
- Broadcast tipico: `192.168.1.255`
- Timeout tipico: `2s`
- Frame sensori: fino a 8 valori, fine frame con `255`

## Modalita mock

Nel pannello laterale abilita `Modalita mock (simulazione)` per testare:

- discovery controller simulato
- risposta versione simulata
- frame sensori simulati
- timeout ed errore `ser` configurabili

Scenari disponibili:

- `normal`: segnali stabili con rumore leggero
- `warmup`: crescita graduale dei valori nelle prime letture
- `drift`: deriva lenta nel tempo
- `spike`: picchi o drop anomali su singoli sensori
- `burst_loss`: raffiche di timeout consecutive

## Struttura file

- `app.py`: UI Streamlit
- `udp_client.py`: logica UDP
- `storage.py`: persistenza SQLite
- `requirements.txt`: dipendenze Python
