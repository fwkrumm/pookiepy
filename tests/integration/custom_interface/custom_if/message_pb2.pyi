import datetime

from google.protobuf import timestamp_pb2 as _timestamp_pb2
from google.protobuf import struct_pb2 as _struct_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class Message(_message.Message):
    __slots__ = ("metaInfo", "history", "payload")
    METAINFO_FIELD_NUMBER: _ClassVar[int]
    HISTORY_FIELD_NUMBER: _ClassVar[int]
    PAYLOAD_FIELD_NUMBER: _ClassVar[int]
    metaInfo: MetaInformation
    history: _containers.RepeatedCompositeFieldContainer[DataPoint]
    payload: Payload
    def __init__(self, metaInfo: _Optional[_Union[MetaInformation, _Mapping]] = ..., history: _Optional[_Iterable[_Union[DataPoint, _Mapping]]] = ..., payload: _Optional[_Union[Payload, _Mapping]] = ...) -> None: ...

class MetaInformation(_message.Message):
    __slots__ = ("timestamp", "messageId", "responseToId", "clientInfo", "serverInfo", "messageName")
    TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    MESSAGEID_FIELD_NUMBER: _ClassVar[int]
    RESPONSETOID_FIELD_NUMBER: _ClassVar[int]
    CLIENTINFO_FIELD_NUMBER: _ClassVar[int]
    SERVERINFO_FIELD_NUMBER: _ClassVar[int]
    MESSAGENAME_FIELD_NUMBER: _ClassVar[int]
    timestamp: _timestamp_pb2.Timestamp
    messageId: str
    responseToId: str
    clientInfo: ClientProvides
    serverInfo: ServerProvides
    messageName: str
    def __init__(self, timestamp: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., messageId: _Optional[str] = ..., responseToId: _Optional[str] = ..., clientInfo: _Optional[_Union[ClientProvides, _Mapping]] = ..., serverInfo: _Optional[_Union[ServerProvides, _Mapping]] = ..., messageName: _Optional[str] = ...) -> None: ...

class DataPoint(_message.Message):
    __slots__ = ("name", "receiveTimestamp", "sendTimestamp", "perfCounter")
    NAME_FIELD_NUMBER: _ClassVar[int]
    RECEIVETIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    SENDTIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    PERFCOUNTER_FIELD_NUMBER: _ClassVar[int]
    name: str
    receiveTimestamp: _timestamp_pb2.Timestamp
    sendTimestamp: _timestamp_pb2.Timestamp
    perfCounter: float
    def __init__(self, name: _Optional[str] = ..., receiveTimestamp: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., sendTimestamp: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., perfCounter: _Optional[float] = ...) -> None: ...

class ClientProvides(_message.Message):
    __slots__ = ("uuid", "name", "requires", "provides")
    UUID_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    REQUIRES_FIELD_NUMBER: _ClassVar[int]
    PROVIDES_FIELD_NUMBER: _ClassVar[int]
    uuid: str
    name: str
    requires: _containers.RepeatedScalarFieldContainer[str]
    provides: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, uuid: _Optional[str] = ..., name: _Optional[str] = ..., requires: _Optional[_Iterable[str]] = ..., provides: _Optional[_Iterable[str]] = ...) -> None: ...

class ServerProvides(_message.Message):
    __slots__ = ("serverId", "uuid", "name")
    SERVERID_FIELD_NUMBER: _ClassVar[int]
    UUID_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    serverId: str
    uuid: str
    name: str
    def __init__(self, serverId: _Optional[str] = ..., uuid: _Optional[str] = ..., name: _Optional[str] = ...) -> None: ...

class Payload(_message.Message):
    __slots__ = ("onlyAFloatPayload",)
    ONLYAFLOATPAYLOAD_FIELD_NUMBER: _ClassVar[int]
    onlyAFloatPayload: float
    def __init__(self, onlyAFloatPayload: _Optional[float] = ...) -> None: ...
