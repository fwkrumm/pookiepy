
"""
Server-side message routing table.

``DataRegister`` maps message names to per-client notification queues and
fan-outs incoming data to all subscribed clients (excluding the sender).
"""
import threading
import queue
import logging
from dataclasses import dataclass

from pookiepy import message_pb2
from pookiepy.exceptions import GrpcValueError


@dataclass(frozen=True, slots=True)
class DeliveryResult:
    """Delivery outcome for a fan-out attempt."""

    delivered: tuple[str, ...] = ()
    dropped: tuple[str, ...] = ()


class DataRegister:
    """Server-side routing table mapping message names to per-client notification queues."""

    def __init__(
        self,
        logger: logging.Logger,
        queue_warning_threshold: int | None = None,
    ):
        """
        data register for payloads

        Args:
            logger (logger.logging.Logger): logger for class
            max_size (int, optional): max size if payload buffer for each messageName.
                Defaults to MAX_BUFFER_SIZE.
        """

        self._logger = logger
        self._queue_warning_threshold = queue_warning_threshold

        # register will contain messageName -> dict of clientId to notification queue
        self._register: dict[str, dict[str, queue.Queue]] = {}

        # _meta_lock guards _register key-set and _locks key-set
        self._meta_lock = threading.Lock()
        # per-messageName locks guard each inner client_dict independently,
        # allowing concurrent routing for different topics
        self._locks: dict[str, threading.Lock] = {}

    def add_notification_queue_for_message_name(self,
                                               client_id: str,
                                               message_name: str,
                                               notification_queue: queue.Queue) -> None:
        """
        add notification queue for a client for a given message_name

        Args:
            client_id (str): ID of the client adding the notification queue
            message_name (str): message name the client should get notified on in case of new data
            notification_queue (queue.Queue): notification queue

        Returns:
            None
        """
        with self._meta_lock:
            self._register.setdefault(message_name, {})
            self._locks.setdefault(message_name, threading.Lock())
            lock = self._locks[message_name]

        with lock:
            if client_id in self._register[message_name]:
                raise ValueError(f"Client {client_id} already registered for "\
                                f"message_name {message_name}")

            self._register[message_name][client_id] = notification_queue

            self._logger.info(
                "client %s added notification queue for message_name %s which has now size %s",
                client_id, message_name, len(self._register[message_name])
            )

    def remove_notification_queues_for_client(self, client_id: str) -> int:
        """
        on client disconnect the notification queues need to be removed.

        Args:
            client_id (str): ID of the client to remove notification queues for

        Returns:
            removed_queues (int): number of removed queues

        """

        self._logger.debug("removing notification queues for client %s", client_id)

        with self._meta_lock:
            message_names = list(self._register.keys())
            locks_snapshot = dict(self._locks)

        removed_count = 0
        for message_name in message_names:
            lock = locks_snapshot.get(message_name)
            if lock is None:
                continue
            with lock:
                client_dict = self._register.get(message_name)
                if client_dict and client_id in client_dict:
                    del client_dict[client_id]
                    removed_count += 1
                    self._logger.debug("Client %s removed from "\
                                       "notification queue list for message_name %s",
                                       client_id, message_name)

        return removed_count

    def add_data_for_message_name(self,
                                 client_id: str,
                                 message_name: str,
                                 data: message_pb2.Message,
                                 target_client_id: str = None) -> DeliveryResult:
        """
        add payload for given message_name

        Args:
            client_id (str): name of the client adding the data; required to prevent
                              self notification
            message_name (str): message name to add data for
            data (any): payload for grpc clients
            target_client_id (str, optional): if specified, only notify this client. Defaults to
                None, which means all clients will be notified.

        Returns:
            DeliveryResult: dataclass with ``delivered`` and ``dropped`` client id tuples.
                ``dropped`` is non-empty only when a bounded subscriber queue is full or when
                a targeted client id is missing. If both tuples are empty, no notification queue
                exists for this message_name.

        Raises:
            GrpcValueError: if data are not of type message_pb2.Message. the latter is the only
                data format which grpc clients should receive!
        """

        if not isinstance(data, message_pb2.Message):
            # the data from the register are directly yield to grpc clients
            raise GrpcValueError(f"Data is not of type Message but {type(data)}. Data cannot "\
                                 "put to register since they will be forwarded to grpc clients.")

        with self._meta_lock:
            lock = self._locks.get(message_name)

        if lock is None:
            self._logger.debug("No notification queue exists for message_name: %s", message_name)
            return DeliveryResult()

        with lock:
            client_dict = self._register.get(message_name, {})
            if not client_dict:
                # change to debug?
                self._logger.warning(
                    "No notification queue exists for message_name: %s", message_name
                )
                return DeliveryResult()
            subscribers = dict(client_dict)  # shallow copy --- puts happen outside the lock

        if target_client_id:
            self._logger.debug("Adding data for message_name %s for target client %s",
                               message_name, target_client_id)
            q = subscribers.get(target_client_id, None)
            if q is None:
                self._logger.error("Target client %s not found for message_name %s. If you "\
                                   "specify a specific target to notifiy it is expected to exist.",
                                   target_client_id, message_name)
                return DeliveryResult(dropped=(target_client_id,))

            try:
                q.put(data, block=False)
            except queue.Full:
                self._logger.error(
                    "Queue full for target client %s for message_name %s. Data not added.",
                    target_client_id,
                    message_name,
                )
                return DeliveryResult(dropped=(target_client_id,))

            self._warn_if_queue_growing(target_client_id, message_name, q)
            return DeliveryResult(delivered=(target_client_id,))


        return_ok = []
        return_nok = []

        # make sure to call put outside of the lock to prevent blocking other threads
        for name, q in subscribers.items():
            if name == client_id:
                # if a client sends data of a specific name and also required that data (which
                # might be the case for some use cases) do skip that self notification
                self._logger.debug("Skipping self notification for client %s for message_name %s",
                                     client_id, message_name)
                continue

            self._logger.debug("adding data for message_name %s for client %s",
                    message_name, name)

            try:
                q.put(data, block=False)
                return_ok.append(name)
            except queue.Full:
                self._logger.error("Queue full for client %s for message_name "\
                                    "%s. Data not added.", name, message_name)
                return_nok.append(name)
                continue

            self._warn_if_queue_growing(name, message_name, q)

        return DeliveryResult(
            delivered=tuple(return_ok),
            dropped=tuple(return_nok),
        )

    def _warn_if_queue_growing(
        self,
        client_id: str,
        message_name: str,
        notification_queue: queue.Queue,
    ):
        """Emit best-effort queue growth warnings.

        Unbounded queues (maxsize=0) can never raise ``queue.Full``. In that
        default mode this warning is the only built-in signal that consumers are
        falling behind. Bounded queues are required for actual backpressure.
        """
        threshold = self._queue_warning_threshold
        if threshold is None:
            return

        current_size = notification_queue.qsize()
        if current_size > threshold:
            self._logger.warning(
                "Queue size for client %s for message_name %s is %s, which is above the "
                "warning threshold of %s. Bounded queues are required for backpressure; "
                "unbounded queues only emit this warning.",
                client_id,
                message_name,
                current_size,
                threshold,
            )
