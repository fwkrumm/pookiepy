"""Launch the interactive voice client for the LM Studio streaming example."""
from examples.interactive_streaming.VoiceClient import VoiceClient


def main():
    """Run voice client until interrupted."""
    client = VoiceClient("voice-ui", 49999)
    try:
        client.listen_forever()
    except KeyboardInterrupt:
        client.disconnect()


if __name__ == "__main__":
    main()
