import type { ChannelCredentials } from "@grpc/grpc-js";

import {
  DEFAULT_CHANNEL_READY_TIMEOUT_MS,
  DEFAULT_CONNECTION_CHECK_TIMEOUT_MS,
  DEFAULT_WAIT_DONE_ADDITIONAL_SLEEP_MS
} from "./constants";

export interface ClientConfig {
  receiveQueueMaxSize: number;
  connectionCheckTimeoutMs: number;
  channelReadyTimeoutMs: number;
  schemaVersion: string;
  sslCredentials: ChannelCredentials | null;
  extMetadata: ReadonlyArray<readonly [string, string]>;
  compression: number | null;
  grpcOptions: ReadonlyArray<readonly [string, string | number | boolean]>;
  waitDoneAdditionalSleepMs: number;
}

export const defaultClientConfig: ClientConfig = {
  receiveQueueMaxSize: 0,
  connectionCheckTimeoutMs: DEFAULT_CONNECTION_CHECK_TIMEOUT_MS,
  channelReadyTimeoutMs: DEFAULT_CHANNEL_READY_TIMEOUT_MS,
  schemaVersion: "",
  sslCredentials: null,
  extMetadata: [],
  compression: null,
  grpcOptions: [
    ["grpc.keepalive_time_ms", 180_000],
    ["grpc.keepalive_timeout_ms", 10_000],
    ["grpc.keepalive_without_calls", 1]
  ],
  waitDoneAdditionalSleepMs: DEFAULT_WAIT_DONE_ADDITIONAL_SLEEP_MS
};

export function mergeClientConfig(config?: Partial<ClientConfig>): ClientConfig {
  if (!config) {
    return {
      ...defaultClientConfig,
      extMetadata: [...defaultClientConfig.extMetadata],
      grpcOptions: [...defaultClientConfig.grpcOptions]
    };
  }

  return {
    ...defaultClientConfig,
    ...config,
    extMetadata: [...(config.extMetadata ?? defaultClientConfig.extMetadata)],
    grpcOptions: [...(config.grpcOptions ?? defaultClientConfig.grpcOptions)]
  };
}
