// package: message.proto.v3
// file: pookiepy/message.proto

/* tslint:disable */
/* eslint-disable */

import * as grpc from "@grpc/grpc-js";
import * as pookiepy_message_pb from "../pookiepy/message_pb";
import * as google_protobuf_timestamp_pb from "google-protobuf/google/protobuf/timestamp_pb";
import * as google_protobuf_struct_pb from "google-protobuf/google/protobuf/struct_pb";

interface IStreamService extends grpc.ServiceDefinition<grpc.UntypedServiceImplementation> {
    dataChannel: IStreamService_IDataChannel;
}

interface IStreamService_IDataChannel extends grpc.MethodDefinition<pookiepy_message_pb.Message, pookiepy_message_pb.Message> {
    path: "/message.proto.v3.Stream/DataChannel";
    requestStream: true;
    responseStream: true;
    requestSerialize: grpc.serialize<pookiepy_message_pb.Message>;
    requestDeserialize: grpc.deserialize<pookiepy_message_pb.Message>;
    responseSerialize: grpc.serialize<pookiepy_message_pb.Message>;
    responseDeserialize: grpc.deserialize<pookiepy_message_pb.Message>;
}

export const StreamService: IStreamService;

export interface IStreamServer extends grpc.UntypedServiceImplementation {
    dataChannel: grpc.handleBidiStreamingCall<pookiepy_message_pb.Message, pookiepy_message_pb.Message>;
}

export interface IStreamClient {
    dataChannel(): grpc.ClientDuplexStream<pookiepy_message_pb.Message, pookiepy_message_pb.Message>;
    dataChannel(options: Partial<grpc.CallOptions>): grpc.ClientDuplexStream<pookiepy_message_pb.Message, pookiepy_message_pb.Message>;
    dataChannel(metadata: grpc.Metadata, options?: Partial<grpc.CallOptions>): grpc.ClientDuplexStream<pookiepy_message_pb.Message, pookiepy_message_pb.Message>;
}

export class StreamClient extends grpc.Client implements IStreamClient {
    constructor(address: string, credentials: grpc.ChannelCredentials, options?: Partial<grpc.ClientOptions>);
    public dataChannel(options?: Partial<grpc.CallOptions>): grpc.ClientDuplexStream<pookiepy_message_pb.Message, pookiepy_message_pb.Message>;
    public dataChannel(metadata?: grpc.Metadata, options?: Partial<grpc.CallOptions>): grpc.ClientDuplexStream<pookiepy_message_pb.Message, pookiepy_message_pb.Message>;
}
