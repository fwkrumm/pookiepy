"""Launch the interactive voice client for the LM Studio streaming example."""
from examples.interactive_streaming.VoiceClient import VoiceClient


def main():
    client = VoiceClient("voice-ui", 49999)
    try:
        client.listen_forever()
    except KeyboardInterrupt:
        client.disconnect()


if __name__ == "__main__":
    main()