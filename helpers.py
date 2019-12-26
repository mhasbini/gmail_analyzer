import re
from datetime import datetime
from termcolor import colored

def remove_dup_timezone(date_str):
  # Convert 'Tue, 24 Dec 2019 08:25:25 +0000 (UTC)' to 'Tue, 24 Dec 2019 08:25:25 +0000'
  _updated_date = re.sub(r'\s+\(.{1,20}\)$', '', date_str)
  # Convert 'Tue, 24 Dec 2019 08:25:25 +0000' to '24 Dec 2019 08:25:25 +0000'
  _updated_date = re.sub(r'^.{1,4},\s+', '', _updated_date)

  return _updated_date

def convert_date(date_str):
  clean_date = remove_dup_timezone(date_str)

  _val = None

  try:
    _val = datetime.strptime(clean_date, '%d %b %Y %H:%M:%S %z')
  except ValueError:
    try:
      _val = datetime.strptime(clean_date, '%d %b %Y %H:%M:%S %Z')
    except ValueError:
      _val = datetime.strptime(clean_date, '%d %b %Y %H:%M:%S')

  return _val

def reduce_to_date(date_str):
  return convert_date(date_str).strftime('%Y-%m-%d')

def reduce_to_datetime(date_str):
  return convert_date(date_str).strftime('%Y-%m-%d %H:%M:%S')

def reduce_to_time(date_str):
  return convert_date(date_str).strftime('%H')

def reduce_to_year(date_str):
  return int(convert_date(date_str).strftime('%Y'))

def chunks(l, n):
    for i in range(0, len(l), n):
        yield l[i : i + n]


loader_icn = colored('*', 'green')
h1_icn = colored('#', 'red')
h2_icn = colored('##', 'red')

