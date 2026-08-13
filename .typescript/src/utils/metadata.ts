import { Timestamp } from "google-protobuf/google/protobuf/timestamp_pb";

import type * as MessagePb from "../generated/pookiepy/message_pb";

import { randomUUID } from "node:crypto";

const messagePb = require("../generated/pookiepy/message_pb") as typeof MessagePb;

const { MetaInformation, Message } = messagePb;
type Message = MessagePb.Message;

export function setMetadata(message: Message): void {
  const meta = message.getMetainfo() ?? new MetaInformation();

  if (!meta.hasTimestamp()) {
    const timestamp = new Timestamp();
    timestamp.fromDate(new Date());
    meta.setTimestamp(timestamp);
  }

  if (!meta.getMessageid()) {
    meta.setMessageid(randomUUID());
  }

  message.setMetainfo(meta);
}
