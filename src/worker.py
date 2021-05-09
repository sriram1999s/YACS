import json
import sys
from socket import *
import time
import threading
from datetime import datetime
import signal


""" Parsing arguments"""

try:
	port = int(sys.argv[1])
except IndexError:
	print("Worker port not given")
	print("Exiting ......")
	exit()

try:
	work_id = sys.argv[2]
except IndexError:
	print("Worker id not passed")
	print("Exiting ......")
	exit()

""" Done """

""" Class definitions """

class Task:
	def __init__(self, task_id, duration, job_id, worker_id):
		self.worker_id = worker_id
		self.job_id = job_id
		self.task_id = task_id
		self.duration = duration
		self.done = False
		self.arrival_time = datetime.now().timestamp()
		self.end_time = -1

	def print(self):
		print("job_id: ", self.job_id, "worker_id: ", self.worker_id, "task_id: ", self.task_id, "arrival: ",
			  self.arrival_time, "end: ", self.end_time, " duration: ", self.duration, "  done: ", self.done)

	def to_json(self):
		temp = {"job_id": self.job_id, "worker_id": self.worker_id, "task_id": self.task_id,
				"arrival_time": self.arrival_time, "end_time": self.end_time, "duration": self.duration, "done": self.done}
		return temp

""" Class definitions are over """

''' Global variables are declared here '''

thread_dict = {} # A dictionary containing all threads

''' End of global variable declaration '''

''' Handling a Keyboard Interrupt '''

def signal_handler(signal, frame):
	print('-'*60)
	print('Exiting Worker')
	print('-'*60)

	sys.exit(0)

''' End of handler '''

print('-'*60)
print(f"Worker {work_id} ready to recieve tasks from master.py")
print('-'*60)

""" Listener code """

def task_in(port):  # Receives task assigned by the master to this worker
	task_in_socket = socket(AF_INET, SOCK_STREAM)  # Init a TCP socket
	task_in_socket.bind(('', port))  # Listen on port 5000, from requests.py
	task_in_socket.listen(3)
	while True:
		connectionSocket, addr = task_in_socket.accept()
		message = connectionSocket.recv(2048)  # Recieve max of 2048 bytes
		print()
		mssg = json.loads(message)

		received_task = Task(mssg['task_id'], mssg['duration'], mssg['job_id'], mssg['worker_id'])

		print(f"Received task {received_task.task_id} from : {addr}\n")

		thread_dict[f"{received_task.task_id}"] = threading.Thread(target=task_out, args=(received_task,))
		thread_dict[f"{received_task.task_id}"].start()

""" Listener code ends """


""" Updater code """

def task_out(task):  # Takes a task as input and sends an update after its completion to the master
	time.sleep(task.duration)
	task.duration = 0
	task.end_time = datetime.now().timestamp()
	task.done = True
	print('-'*60)
	task.print()
	print('-'*60)
	print('\n')

	send_task = Task.to_json(task)
	message = json.dumps(send_task)

	s = socket()
	# print("created socket")

	try:
		s.connect(('127.0.0.1',5001))
		s.send(message.encode())
		print(f"Sent task {task.task_id} completed...\n")
	except:
		print("Could not connect to master\n")

	s.close()

""" Updater code ends """

""" Running worker """

if __name__ == '__main__':

	signal.signal(signal.SIGINT, signal_handler)

	''' initializing threads, starting, and joining '''
	listening_thread = threading.Thread(target=task_in, args=(port,))
	listening_thread.start()
	listening_thread.join()
