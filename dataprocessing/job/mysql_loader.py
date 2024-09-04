# Path hack.

from argparse import ArgumentParser
from datetime import datetime, timedelta
import sys
import os

PACKAGE_PARENT = '..'
SCRIPT_DIR = os.path.dirname(os.path.realpath(os.path.join(os.getcwd(), os.path.expanduser(__file__))))
sys.path.append(os.path.normpath(os.path.join(SCRIPT_DIR, PACKAGE_PARENT)))

from dataprocessing.processors.mysql_ingestion import MySQLDataProcess

parser = ArgumentParser()
parser.add_argument("-ds", "--start_date", dest="start_date", help="start_date", metavar="DATE_START")
parser.add_argument("-de", "--end_date", dest="end_date", help="end_date", metavar="DATE_END")

args = parser.parse_args()

start_date = datetime.strptime(args.start_date, '%Y-%m-%d')
end_date = datetime.strptime(args.end_date, '%Y-%m-%d')

delta = end_date - start_date
print(delta)

print(f"START TO PROCESSING")
for i in range(delta.days + 1):
    dt = start_date + timedelta(days=i)

    year = dt.year
    month = dt.month
    day = dt.day

    print(dt)

    MySQLDataProcess(year, month, day)()

    print(f"{dt} processed")
    print("=" * 30)