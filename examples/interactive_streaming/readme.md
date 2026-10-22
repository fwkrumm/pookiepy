# interactive_streaming

Bidirektionales Streaming-Chat-Beispiel mit **LM Studio** als LLM-Backend.
Ein Proxy-Client leitet Nutzereingaben an LM Studio weiter und streamt die Antwort Token für Token zurück.

Die Ausgabe hängt vom Modell und von den gewählten Parametern ab.

## Architecture

```
TextClient  ──lm_request──►  GrpcServer  ──lm_request──►  LMProxyClient
            ◄─lm_response_stream──────────────────────────  (HTTP → LM Studio)
             └──────────────►  VoiceClient
```

- **GrpcServer** --- schlichtes `BaseServer`, keine Sonderlogik.
- **LMProxyClient** --- empfängt `lm_request`, ruft LM Studio `/v1/chat/completions` im Streaming-Modus auf und sendet `lm_response_stream`-Chucks mit `done`-Flag zurück.
- **TextClient** --- interaktive CLI; liest Eingaben, sendet `lm_request`, druckt Streaming-Chunks live.
- **VoiceClient** --- abonniert `lm_response_stream` und spricht vollständige Sätze mit einer deutschen Neural-Stimme laut vor.

## Requirements

- LM Studio läuft lokal unter `http://127.0.0.1:1234` und hat ein Modell geladen (Standard: `gemma-4e2b`).
- Der Proxy benötigt `requests`. Mit `uv run --with requests` wird es nur für den Proxy-Prozess installiert. Für die Sprach-UI sind außerdem `edge-tts`, `pygame-ce` (importiert als `pygame`) und `pyttsx3` nötig (siehe `requirements_examples.txt`).
- Standardstimme ist Microsoft `de-DE-KatjaNeural` mit erhöhtem Tempo (`+15%`). Beim Start protokolliert der Client den aktiven Sprachfokus Deutsch (`de-DE`). Neural-TTS benötigt eine Internetverbindung.
- Kurze aufeinanderfolgende Sätze werden gemeinsam synthetisiert. Dadurch entstehen keine langen Pausen zwischen Sätzen wie „Hallo! Wie kann ich helfen?“.
- Wenn Neural-TTS nicht erreichbar ist, meldet `VoiceClient` den konkreten Fehler und aktiviert ausdrücklich den `pyttsx3`-Offline-Fallback. Dieser läuft ohne Internet, klingt aber weniger natürlich. Ist auch `pyttsx3` nicht verfügbar, meldet der Client den Wechsel auf reine Textausgabe.

> Wenn LM Studio nicht läuft, fällt `LMProxyClient` auf einen Offline-Stub zurück, der die Eingabe spiegelt.

Alternative Stimmen können beim Erzeugen von `VoiceClient` über `voice=` gesetzt werden, beispielsweise `de-DE-ConradNeural`.

## How to run

**Option A --- drei separate Terminals:**

```
# Terminal 1: Server + Proxy zusammen
uv run --with requests python examples/interactive_streaming/run_server_proxy.py

# Terminal 2: interaktive Text-UI
uv run python examples/interactive_streaming/run_text_client.py

# Terminal 3: Sprach-UI
uv run --with-requirements requirements_examples.txt python examples/interactive_streaming/run_voice_client.py
```

**Option B --- drei separate Prozesse:**

```
uv run python examples/interactive_streaming/GrpcServerExample.py
uv run --with requests python examples/interactive_streaming/LMProxyClient.py
uv run python examples/interactive_streaming/TextClient.py
uv run --with-requirements requirements_examples.txt python examples/interactive_streaming/VoiceClient.py
```

Eingabe im `TextClient`-Terminal. Die LLM-Antwort kommt live zurück. Mit `exit` oder `quit` beenden.

Wenn nur Sprache gewünscht ist, `run_server_proxy.py` und `run_voice_client.py` starten.

## TL;DR

`run_server_proxy.py` starten, dann `run_text_client.py` oder `run_voice_client.py`. Eingabe senden, gestreamte LLM-Antwort erhalten. LM Studio muss lokal laufen.
