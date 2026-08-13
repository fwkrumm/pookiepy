// GENERATED CODE -- DO NOT EDIT!

'use strict';
var grpc = require('@grpc/grpc-js');
var pookiepy_message_pb = require('../pookiepy/message_pb.js');
var google_protobuf_timestamp_pb = require('google-protobuf/google/protobuf/timestamp_pb.js');
var google_protobuf_struct_pb = require('google-protobuf/google/protobuf/struct_pb.js');

function serialize_message_proto_v3_Message(arg) {
  if (!(arg instanceof pookiepy_message_pb.Message)) {
    throw new Error('Expected argument of type message.proto.v3.Message');
  }
  return Buffer.from(arg.serializeBinary());
}

function deserialize_message_proto_v3_Message(buffer_arg) {
  return pookiepy_message_pb.Message.deserializeBinary(new Uint8Array(buffer_arg));
}


// Communication service
var StreamService = exports.StreamService = {
  // bi-directional sending and receiving of communication messages
dataChannel: {
    path: '/message.proto.v3.Stream/DataChannel',
    requestStream: true,
    responseStream: true,
    requestType: pookiepy_message_pb.Message,
    responseType: pookiepy_message_pb.Message,
    requestSerialize: serialize_message_proto_v3_Message,
    requestDeserialize: deserialize_message_proto_v3_Message,
    responseSerialize: serialize_message_proto_v3_Message,
    responseDeserialize: deserialize_message_proto_v3_Message,
  },
};

exports.StreamClient = grpc.makeGenericClientConstructor(StreamService, 'Stream');
