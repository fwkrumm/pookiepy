import { status } from "@grpc/grpc-js";
import { afterEach, describe, expect, it } from "vitest";

import { BaseClient } from "../src/base-client";
import {
  GrpcConnectionError,
  GrpcEmpty,
  GrpcResourceExhaustedError,
  GrpcValueError
} from "../src/errors";
import type * as MessagePb from "../src/generated/pookiepy/message_pb";
import { startTestServer, type StartedTestServer } from "./test-server";

const messagePb = require("../src/generated/pookiepy/message_pb") as typeof MessagePb;
const { Message, MetaInformation } = messagePb;
type Message = MessagePb.Message;

class TestClient extends BaseClient {
  public readonly yielded: Message[] = [];

  protected override async onDataYield(data: Message): Promise<void> {
    this.yielded.push(data);
  }
}

const activeServers: StartedTestServer[] = [];
const activeClients: BaseClient[] = [];

afterEach(async () => {
  await Promise.allSettled(activeClients.map(async (client) => client.disconnect()));
  activeClients.length = 0;

  await Promise.allSettled(activeServers.map(async (server) => server.shutdown()));
  activeServers.length = 0;
});

describe("BaseClient lifecycle", () => {
  it("handshake success stores serverSessionId", async () => {
    const server = await startTestServer({ firstResponse: "welcome" });
    activeServers.push(server);

    const client = new TestClient({
      port: server.port,
      provides: ["foo"],
      requires: ["bar"]
    });
    activeClients.push(client);

    await client.run();

    expect(client.serverSessionId).not.toBe("");
    expect(client.connectionState).toBe("connected");
  });

  it("fails fast when first response misses serverInfo", async () => {
    const server = await startTestServer({ firstResponse: "wrong" });
    activeServers.push(server);

    const client = new TestClient({
      port: server.port,
      provides: ["foo"]
    });
    activeClients.push(client);

    await expect(client.run()).rejects.toBeInstanceOf(GrpcConnectionError);
  });

  it("times out when welcome not received", async () => {
    const server = await startTestServer({ firstResponse: "none" });
    activeServers.push(server);

    const client = new TestClient({
      port: server.port,
      provides: ["foo"],
      config: {
        connectionCheckTimeoutMs: 200
      }
    });
    activeClients.push(client);

    await expect(client.run()).rejects.toBeInstanceOf(GrpcConnectionError);
  });

  it("propagates fatal stream errors quickly", async () => {
    const server = await startTestServer({
      firstResponse: "welcome"
    });
    activeServers.push(server);

    const client = new TestClient({
      port: server.port,
      provides: ["foo"]
    });
    activeClients.push(client);

    await client.run();

    interface StreamErrorEmitter {
      emit(event: "error", error: { code: number; details: string; message: string }): boolean;
    }

    const stream = (client as unknown as { stream?: StreamErrorEmitter }).stream;
    if (!stream) {
      throw new Error("Expected active stream after run()");
    }

    stream.emit("error", {
      code: status.RESOURCE_EXHAUSTED,
      details: "resource exhausted",
      message: "resource exhausted"
    });

    await expect(client.getData(3_000)).rejects.toBeInstanceOf(
      GrpcResourceExhaustedError
    );
  });

  it("waitDone resolves after stream handoff even without app-level server processing", async () => {
    const server = await startTestServer({
      firstResponse: "welcome",
      ignoreAfterHandshake: true
    });
    activeServers.push(server);

    const client = new TestClient({
      port: server.port,
      provides: ["foo"]
    });
    activeClients.push(client);

    await client.run();

    const message = new Message();
    const meta = new MetaInformation();
    meta.setMessagename("foo");
    message.setMetainfo(meta);

    const yieldedBefore = client.yielded.length;
    client.sendData(message);
    await client.waitDone(0);

    expect(client.yielded.length).toBe(yieldedBefore + 1);
    expect(server.processedAfterHandshakeCount()).toBe(0);
  });

  it("disconnect is race-safe and idempotent", async () => {
    const server = await startTestServer({ firstResponse: "welcome" });
    activeServers.push(server);

    const client = new TestClient({
      port: server.port,
      provides: ["foo"]
    });
    activeClients.push(client);

    await client.run();

    await Promise.all([client.disconnect(), client.disconnect()]);

    expect(client.connectionState).toBe("disconnected");
  });

  it("reconnect resets uuid and session fields", async () => {
    const server = await startTestServer({ firstResponse: "welcome" });
    activeServers.push(server);

    const client = new TestClient({
      port: server.port,
      provides: ["foo"]
    });
    activeClients.push(client);

    await client.run();
    const firstUuid = client.uuid;
    const firstSession = client.serverSessionId;

    await client.disconnect();
    expect(client.serverSessionId).toBe(firstSession);

    await client.run();

    expect(client.uuid).not.toBe(firstUuid);
    expect(client.serverSessionId).not.toBe("");
  });
});

describe("BaseClient guardrails", () => {
  it("sendData validates message name in provides", async () => {
    const server = await startTestServer({ firstResponse: "welcome" });
    activeServers.push(server);

    const client = new TestClient({
      port: server.port,
      provides: ["foo"]
    });
    activeClients.push(client);

    await client.run();

    const message = new Message();
    const meta = new MetaInformation();
    meta.setMessagename("bar");
    message.setMetainfo(meta);

    expect(() => client.sendData(message)).toThrow(GrpcValueError);
  });

  it("getData timeout maps to GrpcEmpty", async () => {
    const server = await startTestServer({ firstResponse: "welcome" });
    activeServers.push(server);

    const client = new TestClient({
      port: server.port,
      provides: ["foo"]
    });
    activeClients.push(client);

    await client.run();

    await expect(client.getData(100)).rejects.toBeInstanceOf(GrpcEmpty);
  });
});
