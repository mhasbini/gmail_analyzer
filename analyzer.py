# Get number of messages groupped by labels

import pickle
import os.path
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
import time
import queue
from progress.spinner import Spinner
from progress.bar import IncrementalBar
from progress.counter import Counter
import collections
from ascii_graph import Pyasciigraph
from termgraph.termgraph import chart
import agate
import warnings
import concurrent.futures
from threading import Event


def chunks(l, n):
    for i in range(0, len(l), n):
        yield l[i : i + n]


_progressPadding = 29


class Service:
    def __init__(self):
        self.scopes = ["https://www.googleapis.com/auth/gmail.readonly"]

    def instance(self):
        service = build("gmail", "v1", credentials=self._get_creds())

        return service

    def _get_creds(self):
        creds = None

        # The file token.pickle stores the user's access and refresh tokens, and is
        # created automatically when the authorization flow completes for the first
        # time.
        if os.path.exists("token.pickle"):
            with open("token.pickle", "rb") as token:
                creds = pickle.load(token)

        # If there are no (valid) credentials available, let the user log in.
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(
                    "credentials.json", self.scopes
                )
                creds = flow.run_local_server(port=0)

            # Save the credentials for the next run
            with open("token.pickle", "wb") as token:
                pickle.dump(creds, token)

        return creds


class Processor:
    # Talk to google api, fetch results and decorate them
    def __init__(self):
        self.service = Service().instance()
        self.user_id = "me"
        self.messagesQueue = collections.deque()
        self.failedMessagesQueue = collections.deque()

    def get_messages(self):
        # Get all messages of user
        # Output format:
        # [{'id': '13c...7', 'threadId': '13c...7'}, ...]

        if os.path.exists("messages.pickle"):
            with open("messages.pickle", "rb") as token:
                messages = pickle.load(token)
                return messages

        # includeSpamTrash
        # labelIds
        # https://developers.google.com/resources/api-libraries/documentation/gmail/v1/python/latest/gmail_v1.users.messages.html#list

        response = self.service.users().messages().list(userId=self.user_id).execute()
        messages = []
        est_max = response["resultSizeEstimate"] * 5

        # progress = IncrementalBar('Fetching messages'.ljust(_progressPadding, ' '), max=est_max, suffix="elapsed: %(elapsed)ds - eta: %(eta)ds")
        progress = Counter("Fetching messages page".ljust(_progressPadding, " "))

        if "messages" in response:
            messages.extend(response["messages"])

        while "nextPageToken" in response:
            page_token = response["nextPageToken"]

            response = (
                self.service.users()
                .messages()
                .list(userId=self.user_id, pageToken=page_token)
                .execute()
            )
            messages.extend(response["messages"])

            progress.next()

        progress.finish()

        return messages

    def process_message(self, request_id, response, exception):
        if exception is not None:
            self.failedMessagesQueue.append(exception.uri)
            return

        headers = response["payload"]["headers"]

        _date = next(
            (header["value"] for header in headers if header["name"] == "Date"), None
        )
        _from = next(
            (header["value"] for header in headers if header["name"] == "From"), None
        )

        self.messagesQueue.append(
            {
                "id": response["id"],
                "labels": response["labelIds"],
                "fields": {"from": _from, "date": _date},
            }
        )

    def get_metadata(self, messages):
        # Get metadata for all messages:
        # 1. Create a batch get message request for all messages
        # 2. Process the returned output
        #
        # Output format:
        # {
        #   'id': '16f....427',
        #   'labels': ['UNREAD', 'CATEGORY_UPDATES', 'INBOX'],
        #   'fields': [
        #     {'name': 'Date', 'value': 'Tue, 24 Dec 2019 22:13:09 +0000'},
        #     {'name': 'From', 'value': 'Coursera <no-reply@t.mail.coursera.org>'}
        #   ]
        # }

        if os.path.exists("success.pickle"):
            with open("success.pickle", "rb") as token:
                self.messagesQueue = pickle.load(token)
                return

        progress = IncrementalBar(
            "Fetching messages meta data".ljust(_progressPadding, " "),
            max=len(messages),
        )

        for messages_batch in chunks(messages, 250):
            # for messages_batch in [messages[0:1000]]:
            batch = self.service.new_batch_http_request()

            for message in messages_batch:
                msg_id = message["id"]
                batch.add(
                    self.service.users().messages().get(userId=self.user_id, id=msg_id),
                    callback=self.process_message,
                )

            batch.execute()
            progress.next(len(messages_batch))

        progress.finish()


class Analyzer:
    def __init__(self):
        # Ignore warnings about SSL connections
        warnings.simplefilter("ignore", ResourceWarning)

        self.processor = Processor()
        self.user_id = "me"
        self.table = None

    def _load_table(self, event):
        table = agate.Table.from_object(list(self.processor.messagesQueue))

        event.set()

        self.table = table

        return

    def _analyze_from(self, event):
        data = (
            self.table.pivot("fields/from")
            .where(lambda row: row["fields/from"] is not None)
            .order_by("Count", reverse=True)
            .limit(20)
        )

        _values = data.columns.values()

        data_keys = list(_values[0].values())
        data_count = [[i] for i in list(map(int, list(_values[1].values())))]

        event.set()

        return data_keys, data_count

    def analyse(self):
        """
        read from the messages queue, and generate:
        1. Counter for From field
        2. Counter for Time field (by hour)
        """

        # {'id': '16f39fe119ee8427', 'labels': ['UNREAD', 'CATEGORY_UPDATES', 'INBOX'], 'fields': {'from': 'Coursera <no-reply@t.mail.coursera.org>', 'date': 'Tue, 24 Dec 2019 22:13:09 +0000'}}

        with concurrent.futures.ThreadPoolExecutor() as executor:
            progress = Spinner("Loading messages ")

            load_table_event = Event()

            load_table_future = executor.submit(self._load_table, load_table_event)

            while not load_table_event.isSet():
                progress.next()
                time.sleep(.1)

            progress.finish()

            progress = Spinner("Analysing senders ")

            _analyze_from_event = Event()

            _analyze_from_future = executor.submit(self._analyze_from, _analyze_from_event)

            while not _analyze_from_event.isSet():
                progress.next()
                time.sleep(.1)

            data_keys, data_count = _analyze_from_future.result()

            progress.finish()

        print("\n# Senders\n")
        args = {
            "stacked": False,
            "width": 50,
            "no_labels": False,
            "format": "{:<d}",
            "suffix": "",
            "vertical": False,
            "different_scale": False,
        }

        chart(colors=[92], data=data_count, args=args, labels=data_keys)

    def start(self):
        messages = self.processor.get_messages()

        # with open("messages.pickle", "wb") as token:
        #     pickle.dump(messages, token)

        self.processor.get_metadata(messages)

        self.analyse()

if __name__ == "__main__":
    Analyzer().start()
