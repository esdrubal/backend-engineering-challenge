import argparse
parser = argparse.ArgumentParser()
parser.add_argument('--input_file', nargs=1, required=True,  help='translation events file')
parser.add_argument('--window_size', nargs=1, required=True, help='size of the moving average window')
args = parser.parse_args()

import json
import datetime
import Queue

window_size = int(args.window_size[0])
window_queue = Queue.Queue()
current_minute = None
total_duration = 0

def print_average_delivery_time():
	minute_str = current_minute.strftime("%m/%d/%Y, %H:%M:%S")
	avg_time = float(total_duration) / window_queue.qsize() if window_queue.qsize() != 0 else 0
	print('{"date": %s, "average_delivery_time": %s}' % (minute_str, str(avg_time)))

with open(args.input_file[0]) as event_file:  
	for line in event_file:
		event = json.loads(line)
		et = event["time"] = datetime.datetime.strptime(event["timestamp"], '%Y-%m-%d %H:%M:%S.%f')
		event_minute = datetime.datetime(et.year,et.month,et.day,et.hour,et.minute,0)
		if current_minute is None:
			current_minute = event_minute
		#Slide current_minute up to new event time minute
		while current_minute <= event_minute:
			#Print current average delivery time
			print_average_delivery_time()
			#Slide current minute by one minute
			current_minute += datetime.timedelta(minutes=1)
			oldest_minute = current_minute - datetime.timedelta(minutes=window_size)
			#Pop events outside window
			while not window_queue.empty() and window_queue.queue[0]["time"] < oldest_minute:
				old_event = window_queue.get();
				total_duration -= old_event["duration"]
		#Push new event
		window_queue.put(event)
		total_duration += event["duration"]
	#Print last average delivery time
	print_average_delivery_time()