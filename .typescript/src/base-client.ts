import {
  Metadata,
  type ClientDuplexStream,
  type ServiceError
} from "@grpc/grpc-js";
import { randomUUID } from "node:crypto";

import type { ClientConfig } from "./config";
import { mergeClientConfig } from "./config";
import {
  DISCONNECT_WAIT_TIMEOUT_MS,
  RECEIVE_POLL_SLICE_MS,
  SCHEMA_VERSION_METADATA_KEY
} from "./constants";
import {
  ClientExit,
  GrpcConnectionError,
  GrpcEmpty,
  GrpcValueError
} from "./errors";
import type * as MessagePb from "./generated/pookiepy/message_pb";
import { AsyncReceiveQueue } from "./queue/async-receive-queue";
import { JoinableSendQueue } from "./queue/joinable-send-queue";
import { ClientStateMachine } from "./state/client-state";
import { GrpcTransport, isCancelledError, mapGrpcError } from "./transport/grpc-transport";
import { addInitialHistory, appendReceiveHistory, updateLatestHistoryBeforeSend } from "./utils/history";
import { setMetadata } from "./utils/metadata";

const messagePb = require("./generated/pookiepy/message_pb") as typeof MessagePb;
const { ClientProvides, Message, MetaInformation } = messagePb;
type Message = MessagePb.Message;

class StreamError {
  constructor(public readonly error: Error) {}
}

type InboundItem = Message | StreamError;

export interface BaseClientInit {
  port: number;
  name?: string;
  provides?: string[];
  requires?: string[];
  ip?: string;
  config?: Partial<ClientConfig>;
}

/**
 * Base class for gRPC bidirectional-stream clients.
 * Subclass this and override hooks; no gRPC plumbing needed in app code.
 */
export class BaseClient {
  private readonly state = new ClientStateMachine();
  private readonly configData: ClientConfig;

  private transport: GrpcTransport | undefined;
  private stream: ClientDuplexStream<Message, Message> | undefined;
  private sendQueue = new JoinableSendQueue<Message>();
  private receiveQueue: AsyncReceiveQueue<InboundItem>;
  private sendLoopPromise: Promise<void> | undefined;
  private receiveLoopPromise: Promise<void> | undefined;

  public readonly name: string;
  public readonly port: number;
  public readonly ip: string;
  public readonly provides: string[];
  public readonly requires: string[];

  public uuid = "";
  public serverSessionId = "";

  constructor(options: BaseClientInit) {
    this.name = options.name ?? "client";
    this.port = options.port;
    this.ip = options.ip ?? "localhost";
    this.provides = options.provides ?? [];
    this.requires = options.requires ?? [];
    this.configData = mergeClientConfig(options.config);
    this.receiveQueue = new AsyncReceiveQueue<InboundItem>(this.configData.receiveQueueMaxSize);

    this.state.transition("disconnected");
  }

  get config(): ClientConfig {
    return this.configData;
  }

  get connectionState(): string {
    return this.state.state;
  }

  async run(): Promise<void> {
    if (this.state.state === "connecting" || this.state.state === "connected") {
      throw new GrpcConnectionError("Client already connecting or connected");
    }

    this.state.transition("connecting");
    this.resetPerConnectionState();

    const transportOptions: {
      address: string;
      grpcOptions: ReadonlyArray<readonly [string, string | number | boolean]>;
      credentials?: import("@grpc/grpc-js").ChannelCredentials;
      compression?: number;
    } = {
      address: `${this.ip}:${this.port}`,
      grpcOptions: this.configData.grpcOptions
    };

    if (this.configData.sslCredentials) {
      transportOptions.credentials = this.configData.sslCredentials;
    }

    if (this.configData.compression !== null) {
      transportOptions.compression = this.configData.compression;
    }

    this.transport = new GrpcTransport(transportOptions);

    await this.transport.waitForReady(this.configData.channelReadyTimeoutMs);

    const metadata = this.buildMetadata();
    this.stream = this.transport.openDataChannel(metadata);

    this.startReceiveLoop(this.stream);
    this.startSendLoop(this.stream);

    this.enqueueHandshakeMessage();
    await this.waitForHandshakeWelcome();

    this.state.transition("connected");
    await this.onInit();
  }

  async disconnect(): Promise<void> {
    if (this.state.state === "disconnected") {
      return;
    }

    if (this.state.state === "idle") {
      this.state.transition("disconnected");
      return;
    }

    if (this.state.state !== "disconnecting") {
      this.state.transition("disconnecting");
    }

    this.sendQueue.close(true);
    this.receiveQueue.close();

    if (this.stream) {
      this.stream.cancel();
      this.stream = undefined;
    }

    if (this.transport) {
      this.transport.close();
      this.transport = undefined;
    }

    await Promise.race([
      Promise.allSettled([
        this.sendLoopPromise ?? Promise.resolve(),
        this.receiveLoopPromise ?? Promise.resolve()
      ]),
      new Promise((resolve) => setTimeout(resolve, DISCONNECT_WAIT_TIMEOUT_MS))
    ]);

    await this.onShutdown();
    this.state.force("disconnected");
  }

  sendData(data: Message, addHistory = false): void {
    if (!(data instanceof Message)) {
      throw new GrpcValueError("Data must be of type Message");
    }

    const messageName = data.getMetainfo()?.getMessagename() ?? "";
    if (!this.provides.includes(messageName)) {
      throw new GrpcValueError(
        `Message name ${messageName} not in provides list ${this.provides.join(",")}`
      );
    }

    if (addHistory) {
      if (data.getHistoryList().length > 0) {
        throw new GrpcValueError(
          "Data already has history; addHistory cannot be used when history exists"
        );
      }
      addInitialHistory(data, this.name);
    }

    this.sendQueue.enqueue(data);
  }

  async waitDone(additionalSleepMs = this.configData.waitDoneAdditionalSleepMs): Promise<void> {
    if (!this.state.isActive()) {
      throw new GrpcConnectionError("Client is disconnected");
    }

    if (additionalSleepMs < 0) {
      throw new GrpcValueError("additionalSleepMs must be >= 0");
    }

    await this.sendQueue.waitForDrain();

    if (additionalSleepMs > 0) {
      await new Promise((resolve) => setTimeout(resolve, additionalSleepMs));
    }
  }

  async getData(timeoutMs?: number): Promise<Message> {
    if (timeoutMs === 0) {
      const now = this.receiveQueue.getNowait();
      if (!now) {
        throw new GrpcEmpty("No message available");
      }
      if (now instanceof StreamError) {
        throw now.error;
      }
      return now;
    }

    const deadline = timeoutMs === undefined ? undefined : Date.now() + timeoutMs;

    while (this.state.state !== "disconnected") {
      if (deadline !== undefined) {
        const remaining = deadline - Date.now();
        if (remaining <= 0) {
          throw new GrpcEmpty(`Timeout (${timeoutMs}ms) reached while waiting for message`);
        }

        const item = await this.receiveQueue.get(Math.min(RECEIVE_POLL_SLICE_MS, remaining));
        if (!item) {
          continue;
        }
        if (item instanceof StreamError) {
          throw item.error;
        }
        return item;
      }

      const item = await this.receiveQueue.get(RECEIVE_POLL_SLICE_MS);
      if (!item) {
        continue;
      }
      if (item instanceof StreamError) {
        throw item.error;
      }
      return item;
    }

    throw new ClientExit("Client disconnected while waiting for message");
  }

  async spin(timeoutMs?: number): Promise<boolean | unknown> {
    try {
      const data = await this.getData(timeoutMs);
      return await this.onReceive(data);
    } catch (error: unknown) {
      if (error instanceof ClientExit || error instanceof GrpcEmpty) {
        return false;
      }
      throw error;
    }
  }

  async spinForever(timeoutMs?: number): Promise<void> {
    while (this.state.state !== "disconnected") {
      const result = await this.spin(timeoutMs);
      if (result === false) {
        break;
      }
    }
  }

  protected async onInit(): Promise<void> {
    // subclass hook
  }

  protected async onDataYield(_data: Message): Promise<void> {
    // subclass hook
  }

  protected async onShutdown(): Promise<void> {
    // subclass hook
  }

  protected async onReceive(_data: Message): Promise<unknown> {
    return true;
  }

  private resetPerConnectionState(): void {
    this.uuid = randomUUID();
    this.serverSessionId = "";
    this.sendQueue = new JoinableSendQueue<Message>();
    this.receiveQueue = new AsyncReceiveQueue<InboundItem>(this.configData.receiveQueueMaxSize);
  }

  private buildMetadata(): Metadata {
    const metadata = new Metadata();
    metadata.set(SCHEMA_VERSION_METADATA_KEY, this.configData.schemaVersion);
    for (const [key, value] of this.configData.extMetadata) {
      metadata.set(key, value);
    }
    return metadata;
  }

  private enqueueHandshakeMessage(): void {
    const meta = new MetaInformation();
    const clientInfo = new ClientProvides();
    clientInfo.setUuid(this.uuid);
    clientInfo.setName(this.name);
    clientInfo.setRequiresList(this.requires);
    clientInfo.setProvidesList(this.provides);
    meta.setClientinfo(clientInfo);

    const message = new Message();
    message.setMetainfo(meta);
    this.sendQueue.enqueue(message);
  }

  private async waitForHandshakeWelcome(): Promise<void> {
    const response = await this.receiveQueue.get(this.configData.connectionCheckTimeoutMs);
    if (!response) {
      await this.disconnect();
      throw new GrpcConnectionError(
        "Did not receive response from server within timeout after connecting"
      );
    }

    if (response instanceof StreamError) {
      await this.disconnect();
      throw response.error;
    }

    const meta = response.getMetainfo();
    if (!meta?.hasServerinfo()) {
      await this.disconnect();
      throw new GrpcConnectionError(
        `Did not receive welcome message as first server response; got messageName=${meta?.getMessagename() ?? ""}`
      );
    }

    this.serverSessionId = meta.getServerinfo()?.getUuid() ?? "";
  }

  private startSendLoop(stream: ClientDuplexStream<Message, Message>): void {
    this.sendLoopPromise = (async () => {
      while (this.state.state !== "disconnecting" && this.state.state !== "disconnected") {
        const message = await this.sendQueue.dequeue();
        if (!message) {
          break;
        }

        updateLatestHistoryBeforeSend(message);
        setMetadata(message);

        await this.safeOnDataYield(message);

        stream.write(message);
        this.sendQueue.markHandoff();
      }

      this.sendQueue.close(true);
    })().catch(async (error: unknown) => {
      this.sendQueue.close(true);
      await this.receiveQueue.put(new StreamError(error instanceof Error ? error : new Error(String(error))));
    });
  }

  private startReceiveLoop(stream: ClientDuplexStream<Message, Message>): void {
    this.receiveLoopPromise = new Promise<void>((resolve) => {
      stream.on("data", (response: Message) => {
        appendReceiveHistory(response, this.name);
        void this.receiveQueue.put(response);
      });

      stream.on("error", (error: ServiceError) => {
        if (isCancelledError(error) && this.state.state === "disconnecting") {
          resolve();
          return;
        }

        const mapped = mapGrpcError(error);
        void this.receiveQueue.put(new StreamError(mapped));
        this.state.force("errored");
        resolve();
      });

      stream.on("end", () => {
        resolve();
      });

      stream.on("close", () => {
        resolve();
      });
    });
  }

  private async safeOnDataYield(data: Message): Promise<void> {
    try {
      await this.onDataYield(data);
    } catch {
      // hook errors must not break stream write path
    }
  }
}
