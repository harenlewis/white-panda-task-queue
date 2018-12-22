import sys
import operator
import time
import csv
import argparse
from threading import Timer
from datetime import datetime, timezone


queue = []
start_time = None
timer = None
next_event_ts = None

def schedule_tasks(csv_file, time):
    """
    Parse csv and maintain a queue based upon timestamp and priority.
    This queue is will be used to schedule tasks.
    """

    global next_event_ts, start_time

    try:
        start_time = int(datetime.strptime(time, '%Y/%m/%d %H:%M').replace(tzinfo=timezone.utc).timestamp())
    except ValueError:
        print("Please enter a valid date time in given format: 2017/02/10 4:59")
        sys.exit(1)

    # reading csv file
    try:
        with open(csv_file, 'r') as csv_file:
            csvreader = csv.reader(csv_file)
            for index, row in enumerate(csvreader):
                if index != 0:
                    event_name = row[0].strip('"')
                    event_ts = datetime.strptime(row[1].strip().replace('"', ''), '%Y/%m/%d %H:%M')
                    event_ts = event_ts.replace(tzinfo=timezone.utc).timestamp()
                    try:
                        priority = int(row[2].strip())
                    except IndexError:
                        priority = 999999

                    queue.append((event_name, int(event_ts), priority))
    
    except (IOError, FileNotFoundError):
        print("Could not read file: {0}. Please input a valid file.".format(csv_file))
        sys.exit(1)

    # Sort the task queue based on time first and then priority
    queue.sort(key=operator.itemgetter(1, 2))
    next_event_ts = queue[0][1]

    start() # start processing the tasks

def start():
    """
    Create a timer object to schedule the tasks.
    """
    global timer
    if len(queue) > 0 and next_event_ts:
        timer = Timer(next_event_ts - start_time, process_task, [queue[0]])
        timer.start()
    else:
        timer.cancel()

def process_task(args):
    """
    Process the schedule task and set the next task ts to be processed.
    """
    global next_event_ts

    queue.pop(0)
    event_time = datetime.utcfromtimestamp(args[1])
    event_time = event_time.strftime('%Y/%m/%d %H:%M')
    event_name = args[0]

    print ("Current time [ {0} ], Event {1} Processed".format(event_time, event_name))
    if len(queue) > 0:
        next_event_ts = queue[0][1]
    else:
        next_event_ts = None
    start()

if __name__ == '__main__':
    csv_file = sys.argv[1:][0]
    time = sys.argv[1:][1]

    if csv_file.split('.')[1] != 'csv':
        print("Could not read file: {0}. Please input a csv file.".format(csv_file))        
        sys.exit(1)
    
    schedule_tasks(csv_file, time)

