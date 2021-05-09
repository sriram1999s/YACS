import os
import os.path
import json
import sys

try:
    config_path = sys.argv[1]
except IndexError:
    print("Config path not passed as arg")
    print("Exiting ......")
    exit()

try:
	schedule_algo = sys.argv[2]
except IndexError:
	print("Scheduling algorithm not specified - [RANDOM, RR, LL]")
	print("Exiting ......")
	exit()

try:
	num_requests = sys.argv[3]
except IndexError:
	print("Number of requests not specified ")
	print("Exiting ......")
	exit()

if __name__=="__main__":

    current_path = os.getcwd()
    img_folder_path = os.path.join(current_path,'img')
    if not os.path.exists(img_folder_path):
        os.makedirs(img_folder_path)

    log_folder_path = os.path.join(current_path,'logs')
    if not os.path.exists(log_folder_path):
        os.makedirs(log_folder_path)


    with open(config_path) as f:
    	config_file = json.load(f)
    f.close()

    for worker in config_file['workers']:
        # spawning workers
        os.system(f"gnome-terminal -- python3 worker.py {worker['port']} {worker['worker_id']}")

    # spawning master
    os.system(f"gnome-terminal -- python3 master.py {config_path} {schedule_algo}")

    # spawning requests generator
    os.system(f"gnome-terminal -- python3 requests_eval.py {num_requests}")
