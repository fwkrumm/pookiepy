// package: message.proto.v3
// file: pookiepy/message.proto

/* tslint:disable */
/* eslint-disable */

import * as jspb from "google-protobuf";
import * as google_protobuf_timestamp_pb from "google-protobuf/google/protobuf/timestamp_pb";
import * as google_protobuf_struct_pb from "google-protobuf/google/protobuf/struct_pb";

export class Message extends jspb.Message { 

    hasMetainfo(): boolean;
    clearMetainfo(): void;
    getMetainfo(): MetaInformation | undefined;
    setMetainfo(value?: MetaInformation): Message;
    clearHistoryList(): void;
    getHistoryList(): Array<DataPoint>;
    setHistoryList(value: Array<DataPoint>): Message;
    addHistory(value?: DataPoint, index?: number): DataPoint;

    hasPayload(): boolean;
    clearPayload(): void;
    getPayload(): Payload | undefined;
    setPayload(value?: Payload): Message;

    serializeBinary(): Uint8Array;
    toObject(includeInstance?: boolean): Message.AsObject;
    static toObject(includeInstance: boolean, msg: Message): Message.AsObject;
    static extensions: {[key: number]: jspb.ExtensionFieldInfo<jspb.Message>};
    static extensionsBinary: {[key: number]: jspb.ExtensionFieldBinaryInfo<jspb.Message>};
    static serializeBinaryToWriter(message: Message, writer: jspb.BinaryWriter): void;
    static deserializeBinary(bytes: Uint8Array): Message;
    static deserializeBinaryFromReader(message: Message, reader: jspb.BinaryReader): Message;
}

export namespace Message {
    export type AsObject = {
        metainfo?: MetaInformation.AsObject,
        historyList: Array<DataPoint.AsObject>,
        payload?: Payload.AsObject,
    }
}

export class MetaInformation extends jspb.Message { 

    hasTimestamp(): boolean;
    clearTimestamp(): void;
    getTimestamp(): google_protobuf_timestamp_pb.Timestamp | undefined;
    setTimestamp(value?: google_protobuf_timestamp_pb.Timestamp): MetaInformation;
    getMessageid(): string;
    setMessageid(value: string): MetaInformation;
    getResponsetoid(): string;
    setResponsetoid(value: string): MetaInformation;

    hasClientinfo(): boolean;
    clearClientinfo(): void;
    getClientinfo(): ClientProvides | undefined;
    setClientinfo(value?: ClientProvides): MetaInformation;

    hasServerinfo(): boolean;
    clearServerinfo(): void;
    getServerinfo(): ServerProvides | undefined;
    setServerinfo(value?: ServerProvides): MetaInformation;
    getMessagename(): string;
    setMessagename(value: string): MetaInformation;

    serializeBinary(): Uint8Array;
    toObject(includeInstance?: boolean): MetaInformation.AsObject;
    static toObject(includeInstance: boolean, msg: MetaInformation): MetaInformation.AsObject;
    static extensions: {[key: number]: jspb.ExtensionFieldInfo<jspb.Message>};
    static extensionsBinary: {[key: number]: jspb.ExtensionFieldBinaryInfo<jspb.Message>};
    static serializeBinaryToWriter(message: MetaInformation, writer: jspb.BinaryWriter): void;
    static deserializeBinary(bytes: Uint8Array): MetaInformation;
    static deserializeBinaryFromReader(message: MetaInformation, reader: jspb.BinaryReader): MetaInformation;
}

export namespace MetaInformation {
    export type AsObject = {
        timestamp?: google_protobuf_timestamp_pb.Timestamp.AsObject,
        messageid: string,
        responsetoid: string,
        clientinfo?: ClientProvides.AsObject,
        serverinfo?: ServerProvides.AsObject,
        messagename: string,
    }
}

export class DataPoint extends jspb.Message { 
    getName(): string;
    setName(value: string): DataPoint;

    hasReceivetimestamp(): boolean;
    clearReceivetimestamp(): void;
    getReceivetimestamp(): google_protobuf_timestamp_pb.Timestamp | undefined;
    setReceivetimestamp(value?: google_protobuf_timestamp_pb.Timestamp): DataPoint;

    hasSendtimestamp(): boolean;
    clearSendtimestamp(): void;
    getSendtimestamp(): google_protobuf_timestamp_pb.Timestamp | undefined;
    setSendtimestamp(value?: google_protobuf_timestamp_pb.Timestamp): DataPoint;
    getPerfcounter(): number;
    setPerfcounter(value: number): DataPoint;

    serializeBinary(): Uint8Array;
    toObject(includeInstance?: boolean): DataPoint.AsObject;
    static toObject(includeInstance: boolean, msg: DataPoint): DataPoint.AsObject;
    static extensions: {[key: number]: jspb.ExtensionFieldInfo<jspb.Message>};
    static extensionsBinary: {[key: number]: jspb.ExtensionFieldBinaryInfo<jspb.Message>};
    static serializeBinaryToWriter(message: DataPoint, writer: jspb.BinaryWriter): void;
    static deserializeBinary(bytes: Uint8Array): DataPoint;
    static deserializeBinaryFromReader(message: DataPoint, reader: jspb.BinaryReader): DataPoint;
}

export namespace DataPoint {
    export type AsObject = {
        name: string,
        receivetimestamp?: google_protobuf_timestamp_pb.Timestamp.AsObject,
        sendtimestamp?: google_protobuf_timestamp_pb.Timestamp.AsObject,
        perfcounter: number,
    }
}

export class ClientProvides extends jspb.Message { 
    getUuid(): string;
    setUuid(value: string): ClientProvides;
    getName(): string;
    setName(value: string): ClientProvides;
    clearRequiresList(): void;
    getRequiresList(): Array<string>;
    setRequiresList(value: Array<string>): ClientProvides;
    addRequires(value: string, index?: number): string;
    clearProvidesList(): void;
    getProvidesList(): Array<string>;
    setProvidesList(value: Array<string>): ClientProvides;
    addProvides(value: string, index?: number): string;

    serializeBinary(): Uint8Array;
    toObject(includeInstance?: boolean): ClientProvides.AsObject;
    static toObject(includeInstance: boolean, msg: ClientProvides): ClientProvides.AsObject;
    static extensions: {[key: number]: jspb.ExtensionFieldInfo<jspb.Message>};
    static extensionsBinary: {[key: number]: jspb.ExtensionFieldBinaryInfo<jspb.Message>};
    static serializeBinaryToWriter(message: ClientProvides, writer: jspb.BinaryWriter): void;
    static deserializeBinary(bytes: Uint8Array): ClientProvides;
    static deserializeBinaryFromReader(message: ClientProvides, reader: jspb.BinaryReader): ClientProvides;
}

export namespace ClientProvides {
    export type AsObject = {
        uuid: string,
        name: string,
        requiresList: Array<string>,
        providesList: Array<string>,
    }
}

export class ServerProvides extends jspb.Message { 
    getServerid(): string;
    setServerid(value: string): ServerProvides;
    getUuid(): string;
    setUuid(value: string): ServerProvides;
    getName(): string;
    setName(value: string): ServerProvides;

    serializeBinary(): Uint8Array;
    toObject(includeInstance?: boolean): ServerProvides.AsObject;
    static toObject(includeInstance: boolean, msg: ServerProvides): ServerProvides.AsObject;
    static extensions: {[key: number]: jspb.ExtensionFieldInfo<jspb.Message>};
    static extensionsBinary: {[key: number]: jspb.ExtensionFieldBinaryInfo<jspb.Message>};
    static serializeBinaryToWriter(message: ServerProvides, writer: jspb.BinaryWriter): void;
    static deserializeBinary(bytes: Uint8Array): ServerProvides;
    static deserializeBinaryFromReader(message: ServerProvides, reader: jspb.BinaryReader): ServerProvides;
}

export namespace ServerProvides {
    export type AsObject = {
        serverid: string,
        uuid: string,
        name: string,
    }
}

export class Payload extends jspb.Message { 

    hasStructpayload(): boolean;
    clearStructpayload(): void;
    getStructpayload(): google_protobuf_struct_pb.Struct | undefined;
    setStructpayload(value?: google_protobuf_struct_pb.Struct): Payload;
    getBytepayload(): Uint8Array | string;
    getBytepayload_asU8(): Uint8Array;
    getBytepayload_asB64(): string;
    setBytepayload(value: Uint8Array | string): Payload;

    serializeBinary(): Uint8Array;
    toObject(includeInstance?: boolean): Payload.AsObject;
    static toObject(includeInstance: boolean, msg: Payload): Payload.AsObject;
    static extensions: {[key: number]: jspb.ExtensionFieldInfo<jspb.Message>};
    static extensionsBinary: {[key: number]: jspb.ExtensionFieldBinaryInfo<jspb.Message>};
    static serializeBinaryToWriter(message: Payload, writer: jspb.BinaryWriter): void;
    static deserializeBinary(bytes: Uint8Array): Payload;
    static deserializeBinaryFromReader(message: Payload, reader: jspb.BinaryReader): Payload;
}

export namespace Payload {
    export type AsObject = {
        structpayload?: google_protobuf_struct_pb.Struct.AsObject,
        bytepayload: Uint8Array | string,
    }
}
