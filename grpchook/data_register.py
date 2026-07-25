
"""
Server-side message routing table.

``DataRegister`` maps message names to per-client notification queues and
fan-outs incoming data to all subscribed clients (excluding the sender).
"""
import threading
import queue
import logging

from grpchook import message_pb2
from grpchook.exceptions import GrpcValueError

# make configurable? currently no specific reason for the value
WARNING_AT_QUEUE_SIZE = 100_000

class DataRegister():
    """Server-side routing table mapping message names to per-client notification queues."""

    def __init__(self, logger: logging.Logger):
        """
        data register for payloads

        Args:
            logger (logger.logging.Logger): logger for class
            max_size (int, optional): max size if payload buffer for each messageName.
                Defaults to MAX_BUFFER_SIZE.
        """

        self._logger = logger

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
                                               notification_queue: queue.Queue) -> int:
        """
        add notification queue for a client for a given message_name

        Args:
            client_id (str): ID of the client adding the notification queue
            message_name (str): message name the client should get notified on in case of new data
            notification_queue (queue.Queue): notification queue

        Returns:
            int: position of notification queue within message_name. server keeps track of this.
                required so that in case of client disconnect the queue can be removed
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
                                 target_client_id: str = None) -> tuple[tuple[str], tuple[str]]:
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
            tuple[tuple[str], tuple[str]]: tuple of two tuples,
                first tuple contains client_ids for which data was added successfully,
                second tuple contains client_ids for which data could not be added
                    (e.g. because queue was full)
                if both are empty, then no notification queue exists for this message_name

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
            return ((), ())

        with lock:
            client_dict = self._register.get(message_name, {})
            if not client_dict:
                # change to debug?
                self._logger.warning(
                    "No notification queue exists for message_name: %s", message_name
                )
                return ((), ())
            subscribers = dict(client_dict)  # shallow copy --- puts happen outside the lock

        if target_client_id:
            self._logger.debug("Adding data for message_name %s for target client %s",
                               message_name, target_client_id)
            q = subscribers.get(target_client_id, None)
            if q is None:
                self._logger.error("Target client %s not found for message_name %s. If you "\
                                   "specify a specific target to notifiy it is expected to exist.",
                                   target_client_id, message_name)
                return ((), (target_client_id,))

            # queue found, add data
            q.put(data, block=False)
            return ((target_client_id,), ())


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
                # should we catch other errors?
                self._logger.error("Queue full for client %s for message_name "\
                                    "%s. Data not added.", name, message_name)
                return_nok.append(name)

            if q.qsize() > WARNING_AT_QUEUE_SIZE:
                self._logger.warning("Queue size for client %s for message_name %s is "
                                     "%s, which is above the warning threshold of %s. Consider "\
                                     "increasing the threshold or check if clients consume data.",
                                     name, message_name, q.qsize(), WARNING_AT_QUEUE_SIZE)

        return (tuple(return_ok), tuple(return_nok))
