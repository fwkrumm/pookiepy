import {
  ChannelCredentials,
  ClientDuplexStream,
  ClientOptions,
  credentials,
  Metadata,
  status,
  type ServiceError
} from "@grpc/grpc-js";

import type * as MessagePb from "../generated/pookiepy/message_pb";
import type * as MessageGrpcPb from "../generated/pookiepy/message_grpc_pb";
import {
  GrpcConnectionError,
  GrpcResourceExhaustedError,
  GrpcTimeoutError
} from "../errors";

const messageGrpcPb = require("../generated/pookiepy/message_grpc_pb") as typeof MessageGrpcPb;

const { StreamClient } = messageGrpcPb;
type Message = MessagePb.Message;

export interface TransportOptions {
  address: string;
  credentials?: ChannelCredentials;
  grpcOptions: ReadonlyArray<readonly [string, string | number | boolean]>;
  compression?: number;
}

export class GrpcTransport {
  private readonly client: InstanceType<typeof StreamClient>;

  constructor(options: TransportOptions) {
    const clientOptions: Partial<ClientOptions> = {
      ...Object.fromEntries(options.grpcOptions)
    };

    if (options.compression !== undefined) {
      clientOptions["grpc.default_compression_algorithm"] = options.compression;
    }

    this.client = new StreamClient(
      options.address,
      options.credentials ?? credentials.createInsecure(),
      clientOptions
    );
  }

  async waitForReady(timeoutMs: number): Promise<void> {
    await new Promise<void>((resolve, reject) => {
      this.client.waitForReady(Date.now() + timeoutMs, (error?: Error) => {
        if (error) {
          reject(new GrpcConnectionError(error.message));
          return;
        }
        resolve();
      });
    });
  }

  openDataChannel(metadata: Metadata): ClientDuplexStream<Message, Message> {
    return this.client.dataChannel(metadata);
  }

  close(): void {
    this.client.close();
  }
}

export function mapGrpcError(error: ServiceError): Error {
  if (error.code === status.DEADLINE_EXCEEDED) {
    return new GrpcTimeoutError(error.details || "Deadline exceeded");
  }

  if (error.code === status.RESOURCE_EXHAUSTED) {
    return new GrpcResourceExhaustedError(error.details || "Resource exhausted");
  }

  if (error.code === status.CANCELLED) {
    return new GrpcConnectionError(error.details || "Stream cancelled");
  }

  return new GrpcConnectionError(error.details || error.message);
}

export function isCancelledError(error: ServiceError): boolean {
  return error.code === status.CANCELLED;
}
