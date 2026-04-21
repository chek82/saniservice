# User Guide - Saniservice App Discovery

Questa guida descrive come usare l'app Streamlit per:
- raccolta dati sensori UDP
- import misurazioni da Google Drive
- analisi termica
- generazione report PDF

## 1. Requisiti

- Python 3.10+
- Ambiente virtuale attivo
- Dipendenze installate da requirements

## 2. Avvio applicazione

Dal folder progetto:

```powershell
cd app_discovery
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
streamlit run app.py
```

## 3. Accesso con password

All'avvio viene mostrata la schermata login.

- Inserisci la password applicativa.
- Dopo login corretto, la sessione resta in cache per evitare login continuo.
- Per uscire usa il pulsante Logout nella barra laterale.

Nota: la password non e salvata in chiaro nel codice, ma verificata tramite hash PBKDF2.

## 4. Navigazione generale

La sidebar e inizialmente chiusa (comportamento voluto).

In sidebar trovi:
- Logout
- Toggle Advance
- Settings (path locale Drive e URL Drive)

### Advance mode

- Advance OFF: visibile solo tab Report Sanificazione.
- Advance ON: visibili anche tab Collector UDP e Send data.

## 5. Tab Report Sanificazione

### 5.1 Anagrafica intervento

Compila i campi principali:
- Cliente
- Indirizzo
- Data intervento
- Luogo intervento
- Tecnico
- Codice intervento
- Oggetto trattato
- Note

### 5.2 Sorgente dati

Sono disponibili:
- CSV
- Manuale
- Storico collector
- Import from Drive

#### CSV
Carica un file con colonne:
- tempo_min
- temperatura_c

#### Manuale
Inserisci righe nel formato:
- tempo_min,temperatura_c

#### Storico collector
- Seleziona codice attivita
- Scegli metrica (media, max, s1..s8)

#### Import from Drive
Hai due origini:
- Cartella locale
- URL Google Drive

##### Cartella locale
Inserisci il path locale sincronizzato Google Drive.

##### URL Google Drive
- Incolla il link cartella condivisa
- Premi Scarica/aggiorna da URL
- Seleziona il JSON dalla lista

## 6. Sezione File disponibili (Import Drive)

Funzioni principali:
- filtro per nome file
- ordinamento (nome/data, e su desktop anche size)
- paginazione (10 elementi)

### Comportamento responsive

- Desktop: tabella con nome, data creazione, size e selezione classica.
- Mobile: layout semplificato (senza size) con selezione tramite checkbox per riga.

## 7. Metrica temperatura da JSON

Metrica selezionabile:
- media_sensori
- max_sensori
- p1..p6
- s1, s2
- sonde_s1_s2 (Sonde S1 e S2)

Quando scegli sonde_s1_s2:
- il grafico in app mostra due linee (S1 e S2)
- per i calcoli automatici viene usata la media S1/S2

## 8. Analisi termica

Dopo il caricamento dati:
- temperatura massima
- minuti sopra soglia
- soglia raggiunta
- esito conforme/non conforme

Puoi configurare:
- soglia letale
- minuti minimi sopra soglia

## 9. Generazione PDF

Prima del pulsante Scarica report PDF trovi i flag:
- Grafico Temperatura
- Tabella Temperatura
- Grafico 8 Sensori

Il PDF includera solo le sezioni abilitate.

Inoltre il report riporta anche:
- ora prima misurazione
- ora fine misurazione
- durata intervento

## 10. Tab Collector UDP (solo Advance ON)

Funzioni:
- discovery controller
- lettura versione
- polling singolo
- batch acquisizione
- storico frame

Modalita mock disponibile per test senza hardware.

## 11. Tab Send data (solo Advance ON)

Permette invio periodico UDP simulato con:
- diversi formati payload
- profili valori (statico/ramp/random)
- start/stop simulatore

## 12. Settings

In sidebar sezione Settings:
- Google Drive folder path
- Google Drive folder URL
- Salva settings

I valori vengono persistiti su file settings locale.

## 13. Troubleshooting rapido

### Errore "Repository not found" su push git
Verifica:
- repo GitHub esistente
- URL remote corretto
- permessi account/token

### Errore path Drive non valido
- se usi URL, seleziona origine URL Google Drive
- se usi locale, inserisci path filesystem reale (non link web)

### Nessun JSON trovato
- controlla cartella/link
- usa Scarica/aggiorna da URL
- verifica estensione .json

## 14. File principali

- app.py: UI Streamlit e flussi applicativi
- report_utils.py: analisi e PDF
- storage.py: persistenza SQLite
- udp_client.py: protocollo UDP/mock
- requirements.txt: dipendenze
