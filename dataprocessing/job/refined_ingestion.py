# Path hack.

from argparse import ArgumentParser
from datetime import datetime, timedelta
import sys
import os

PACKAGE_PARENT = '..'
SCRIPT_DIR = os.path.dirname(os.path.realpath(os.path.join(os.getcwd(), os.path.expanduser(__file__))))
sys.path.append(os.path.normpath(os.path.join(SCRIPT_DIR, PACKAGE_PARENT)))

from dataprocessing.processors.refined_ingestion import BusLineRefinedProcess, BusItineraryRefinedProcess, BusTrackingRefinedMultithreadProcess

parser = ArgumentParser()
parser.add_argument("-ds", "--start_date", dest="start_date", help="start_date", metavar="DATE_START")
parser.add_argument("-de", "--end_date", dest="end_date", help="end_date", metavar="DATE_END")
parser.add_argument("-j", "--job", dest="job", help="job", metavar="JOB")

args = parser.parse_args()

start_date = datetime.strptime(args.start_date, '%Y-%m-%d')
end_date = datetime.strptime(args.end_date, '%Y-%m-%d')

job = args.job

delta = end_date - start_date
print(delta)

print(f"START TO PROCESSING {job}")
for i in range(delta.days + 1):
    dt = start_date + timedelta(days=i)

    year = dt.year
    month = dt.month
    day = dt.day

    print(dt)

    if job == 'line':
        BusLineRefinedProcess(year, month, day)()
    elif job == 'itinerary':
        BusItineraryRefinedProcess(year, month, day)()
    elif job == 'tracking':
        BusTrackingRefinedMultithreadProcess(year, month, day, 5)()
    else:
        raise NotImplementedError("Job not implemented yet...")

    print(f"{job} :: {dt} processed")
    print("=" * 30)