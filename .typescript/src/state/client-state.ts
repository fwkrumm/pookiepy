import { GrpcConnectionError } from "../errors";

export type ClientState =
  | "idle"
  | "connecting"
  | "connected"
  | "disconnecting"
  | "disconnected"
  | "errored";

const ALLOWED_TRANSITIONS: Record<ClientState, ReadonlySet<ClientState>> = {
  idle: new Set(["connecting", "disconnected"]),
  connecting: new Set(["connected", "disconnecting", "errored", "disconnected"]),
  connected: new Set(["disconnecting", "errored"]),
  disconnecting: new Set(["disconnected", "errored"]),
  disconnected: new Set(["connecting"]),
  errored: new Set(["disconnecting", "disconnected", "connecting"])
};

export class ClientStateMachine {
  private current: ClientState = "idle";

  get state(): ClientState {
    return this.current;
  }

  transition(next: ClientState): void {
    const allowed = ALLOWED_TRANSITIONS[this.current];
    if (!allowed.has(next)) {
      throw new GrpcConnectionError(
        `Invalid client state transition: ${this.current} -> ${next}`
      );
    }
    this.current = next;
  }

  force(next: ClientState): void {
    this.current = next;
  }

  isActive(): boolean {
    return this.current === "connecting" || this.current === "connected";
  }
}
