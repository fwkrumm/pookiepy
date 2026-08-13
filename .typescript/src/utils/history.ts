import { performance } from "node:perf_hooks";

import { Timestamp } from "google-protobuf/google/protobuf/timestamp_pb";

import type * as MessagePb from "../generated/pookiepy/message_pb";

const messagePb = require("../generated/pookiepy/message_pb") as typeof MessagePb;

const { DataPoint } = messagePb;
type Message = MessagePb.Message;

export function appendReceiveHistory(message: Message, name: string): void {
  if (message.getHistoryList().length === 0) {
    return;
  }

  const point = new DataPoint();
  point.setName(name);
  const timestamp = new Timestamp();
  timestamp.fromDate(new Date());
  point.setReceivetimestamp(timestamp);
  point.setPerfcounter(performance.now());
  message.addHistory(point);
}

export function updateLatestHistoryBeforeSend(message: Message): void {
  const history = message.getHistoryList();
  if (history.length === 0) {
    return;
  }

  const latest = history[history.length - 1];
  if (!latest) {
    return;
  }

  const elapsed = performance.now() - latest.getPerfcounter();
  latest.setPerfcounter(elapsed);

  const sendTs = new Timestamp();
  sendTs.fromDate(new Date());
  latest.setSendtimestamp(sendTs);
}

export function addInitialHistory(message: Message, name: string): void {
  const point = new DataPoint();
  point.setName(name);
  const receiveTs = new Timestamp();
  receiveTs.fromDate(new Date());
  point.setReceivetimestamp(receiveTs);
  point.setPerfcounter(performance.now());
  message.addHistory(point);
}
