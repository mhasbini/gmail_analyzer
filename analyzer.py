# Get number of messages groupped by labels

import time
from progress.spinner import Spinner
from ascii_graph import Pyasciigraph
from termgraph.termgraph import chart, calendar_heatmap
import agate
import warnings
import concurrent.futures
from threading import Event
import termtables
import colorama
import helpers

from processor import Processor

class Analyzer:
    def __init__(self):
        # Ignore warnings about SSL connections
        warnings.simplefilter("ignore", ResourceWarning)

        self.processor = Processor()
        self.user_id = "me"
        self.resultsLimit = 20
        self.table = None

    def _load_table(self, event):
        table = agate.Table.from_object(list(self.processor.messagesQueue))

        event.set()

        self.table = table

        return

    def _analyze_senders(self, event):
        data = (
            self.table.pivot("fields/from")
            .where(lambda row: row["fields/from"] is not None)
            .order_by("Count", reverse=True)
            .limit(self.resultsLimit)
        )

        _values = data.columns.values()

        data_keys = list(_values[0].values())
        data_count = [[i] for i in list(map(int, list(_values[1].values())))]

        event.set()

        print(f"\n\n{helpers.h1_icn} Senders\n")
        args = {
            "stacked": False,
            "width": 50,
            "no_labels": False,
            "format": "{:<,d}",
            "suffix": "",
            "vertical": False,
            "different_scale": False,
        }

        chart(colors=[94], data=data_count, args=args, labels=data_keys)

    def _analyze_count(self, event):
        # Average emails per day
        total = self.table.aggregate([('total', agate.Count())])['total']
        total_senders = self.table.distinct('fields/from').select('fields/from').aggregate([('total', agate.Count())])['total']

        if total == 0:
            first_email_date = ''
            last_email_date = None
        else:
            date_data = self.table.where(lambda row: row["fields/date"] is not None).compute([
                ('reduce_to_datetime', agate.Formula(agate.DateTime(datetime_format='%Y-%m-%d %H:%M:%S'), lambda row: helpers.reduce_to_datetime(row['fields/date'])))
            ])
            first_email_date = date_data.order_by('reduce_to_datetime').limit(1).columns['fields/date'].values()[0]
            last_email_date = date_data.order_by('reduce_to_datetime', reverse=True).limit(1).columns['fields/date'].values()[0]
        event.set()

        metrics = [["Total emails", total], ["Senders", total_senders], ["First Email Date", first_email_date]]

        if last_email_date:
            date_delta = helpers.convert_date(last_email_date) - helpers.convert_date(first_email_date)
            avg_email_per_day = total / date_delta.days
            metrics.append(["Avg. Emails/Day", f"{avg_email_per_day:.2f}"])

        print(f"\n\n{helpers.h1_icn} Stats\n")
        print(termtables.to_string(
            metrics
        ))

    def _analyze_date(self, event):
        table = self.table.where(lambda row: row["fields/date"] is not None).compute([
            ('reduce_to_date', agate.Formula(agate.Text(), lambda row: helpers.reduce_to_date(row['fields/date']))),
            ('reduce_to_year', agate.Formula(agate.Number(), lambda row: helpers.reduce_to_year(row['fields/date']))),
            ('reduce_to_time', agate.Formula(agate.Number(), lambda row: helpers.reduce_to_time(row['fields/date'])))
        ])

        years = table.distinct('reduce_to_year').columns['reduce_to_year'].values()

        _data = {}

        for year in years:
            _data[year] = table.where(lambda row: row['reduce_to_year'] == year).select("reduce_to_date").pivot("reduce_to_date").order_by("reduce_to_date")

        event.set()

        print(f"\n\n{helpers.h1_icn} Date\n")

        for year in years:
            data_keys = list(_data[year].columns['reduce_to_date'].values())
            _counts = list(map(int, list(_data[year].columns['Count'].values())))
            _sum = sum(_counts)
            data_count = [[i] for i in _counts]

            args = {
              'color': False,
              'custom_tick': False,
              'start_dt': f'{year}-01-01',
            }

            print(f"\n{helpers.h2_icn} Year {year} ({_sum:,} emails)\n")
            calendar_heatmap(data=data_count, args=args, labels=data_keys)

    def analyse(self):
        """
        read from the messages queue, and generate:
        1. Counter for From field
        2. Counter for Time field (by hour)
        """

        # {'id': '16f39fe119ee8427', 'labels': ['UNREAD', 'CATEGORY_UPDATES', 'INBOX'], 'fields': {'from': 'Coursera <no-reply@t.mail.coursera.org>', 'date': 'Tue, 24 Dec 2019 22:13:09 +0000'}}

        with concurrent.futures.ThreadPoolExecutor() as executor:
            progress = Spinner(f"{helpers.loader_icn} Loading messages ")

            event = Event()

            future = executor.submit(self._load_table, event)

            while not event.isSet() and future.running():
                progress.next()
                time.sleep(.1)

            progress.finish()

            progress = Spinner(f"{helpers.loader_icn} Analysing count ")

            event = Event()

            future = executor.submit(self._analyze_count, event)

            while not event.isSet() and future.running():
                progress.next()
                time.sleep(.1)

            progress.finish()

            progress = Spinner(f"{helpers.loader_icn} Analysing senders ")

            event = Event()

            future = executor.submit(self._analyze_senders, event)

            while not event.isSet() and future.running():
                progress.next()
                time.sleep(.1)

            progress.finish()

            progress = Spinner(f"{helpers.loader_icn} Analysing dates ")

            event = Event()

            future = executor.submit(self._analyze_date, event)

            while not event.isSet() and future.running():
                progress.next()
                time.sleep(.1)

            progress.finish()

    def start(self):
        messages = self.processor.get_messages()

        self.processor.get_metadata(messages)

        self.analyse()

if __name__ == "__main__":
    colorama.init()

    Analyzer().start()
