import type { Message } from "./generated/pookiepy/message_pb";

/**
 * Hook contract for BaseClient subclasses.
 * Subclasses override methods they need; gRPC plumbing stays in BaseClient.
 */
export interface BaseClientHooks {
  onInit(): void | Promise<void>;
  onReceive(data: Message): unknown | Promise<unknown>;
  onShutdown(): void | Promise<void>;
  onDataYield(data: Message): void | Promise<void>;
}
