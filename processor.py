import collections
import os.path
import pickle
from progress.counter import Counter
from progress.bar import IncrementalBar
import helpers

from service import Service

_progressPadding = 29

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

        # if os.path.exists("messages.pickle"):
        #     with open("messages.pickle", "rb") as token:
        #         messages = pickle.load(token)
        #         return messages

        # includeSpamTrash
        # labelIds
        # https://developers.google.com/resources/api-libraries/documentation/gmail/v1/python/latest/gmail_v1.users.messages.html#list

        response = self.service.users().messages().list(userId=self.user_id).execute()
        messages = []
        est_max = response["resultSizeEstimate"] * 5

        # progress = IncrementalBar('Fetching messages'.ljust(_progressPadding, ' '), max=est_max, suffix="elapsed: %(elapsed)ds - eta: %(eta)ds")
        progress = Counter(f"{helpers.loader_icn} Fetching messages page ".ljust(_progressPadding, " "))

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

        # if os.path.exists("success.pickle"):
        #     with open("success.pickle", "rb") as token:
        #         self.messagesQueue = pickle.load(token)
        #         return

        progress = IncrementalBar(
            f"{helpers.loader_icn} Fetching messages meta data ".ljust(_progressPadding, " "),
            max=len(messages),
        )

        for messages_batch in helpers.chunks(messages, 250):
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
