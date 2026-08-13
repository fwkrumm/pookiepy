import {
  Server,
  ServerCredentials,
  status,
  type ServerDuplexStream
} from "@grpc/grpc-js";

import type * as MessagePb from "../src/generated/pookiepy/message_pb";
import type * as MessageGrpcPb from "../src/generated/pookiepy/message_grpc_pb";

const messagePb = require("../src/generated/pookiepy/message_pb") as typeof MessagePb;
const messageGrpcPb = require("../src/generated/pookiepy/message_grpc_pb") as typeof MessageGrpcPb;

const { Message, MetaInformation, ServerProvides } = messagePb;
const { StreamService } = messageGrpcPb;
type Message = MessagePb.Message;
type IStreamServer = MessageGrpcPb.IStreamServer;

export interface TestServerBehavior {
  firstResponse: "welcome" | "wrong" | "none";
  welcomeDelayMs?: number;
  ignoreAfterHandshake?: boolean;
  fatalAfterHandshakeCode?: status;
  fatalAfterHandshakeDetails?: string;
}

export interface StartedTestServer {
  port: number;
  shutdown(): Promise<void>;
  processedAfterHandshakeCount: () => number;
}

interface GrpcLikeError extends Error {
  code: number;
  details: string;
}

export async function startTestServer(
  behavior: TestServerBehavior
): Promise<StartedTestServer> {
  const server = new Server();
  let processedAfterHandshake = 0;

  const impl: IStreamServer = {
    dataChannel(call: ServerDuplexStream<Message, Message>): void {
      let first = true;

      call.on("data", (msg: Message) => {
        if (first) {
          first = false;
          handleFirstMessage(call, behavior);
          return;
        }

        if (behavior.fatalAfterHandshakeCode !== undefined) {
          const err = new Error(
            behavior.fatalAfterHandshakeDetails ?? "forced server failure"
          ) as GrpcLikeError;
          err.code = behavior.fatalAfterHandshakeCode;
          err.details = behavior.fatalAfterHandshakeDetails ?? "forced server failure";
          call.destroy(err);
          return;
        }

        if (behavior.ignoreAfterHandshake) {
          return;
        }

        processedAfterHandshake += 1;
        call.write(msg);
      });

      call.on("end", () => {
        call.end();
      });
    }
  };

  server.addService(StreamService, impl);

  const port = await new Promise<number>((resolve, reject) => {
    server.bindAsync(
      "127.0.0.1:0",
      ServerCredentials.createInsecure(),
      (error: Error | null, boundPort: number) => {
        if (error) {
          reject(error);
          return;
        }
        resolve(boundPort);
      }
    );
  });

  server.start();

  return {
    port,
    shutdown: async () => {
      await new Promise<void>((resolve) => {
        server.tryShutdown(() => resolve());
      });
    },
    processedAfterHandshakeCount: () => processedAfterHandshake
  };
}

function handleFirstMessage(
  call: ServerDuplexStream<Message, Message>,
  behavior: TestServerBehavior
): void {
  if (behavior.firstResponse === "none") {
    return;
  }

  const delayMs = behavior.welcomeDelayMs ?? 0;

  setTimeout(() => {
    const message = new Message();
    const meta = new MetaInformation();

    if (behavior.firstResponse === "welcome") {
      const serverInfo = new ServerProvides();
      serverInfo.setUuid(`server-${Date.now()}`);
      serverInfo.setName("test-server");
      meta.setServerinfo(serverInfo);
    } else {
      meta.setMessagename("not-welcome");
    }

    message.setMetainfo(meta);
    call.write(message);

    if (behavior.fatalAfterHandshakeCode !== undefined) {
      const err = new Error(
        behavior.fatalAfterHandshakeDetails ?? "forced server failure"
      ) as GrpcLikeError;
      err.code = behavior.fatalAfterHandshakeCode;
      err.details = behavior.fatalAfterHandshakeDetails ?? "forced server failure";
      call.destroy(err);
    }
  }, delayMs);
}
