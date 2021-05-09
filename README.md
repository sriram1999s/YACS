# YACS

Yet Another Centralized Scheduler

### Execution
Code tested and debugged on ubuntu 20.04  

Run the following commands:

```sh
sh run.sh
python3 log_analyser.py
```  

or run

```sh
python3 start_system.py config.json RANDOM 10

```
or explicitly create directories as last resort

```sh
mkdir logs/ img/
```
and run the following commands on 5 different terminals, in the respective order.  

```sh
python3 worker.py 4000 1
python3 worker.py 4001 2
python3 worker.py 4002 3
python3 master.py config.json RANDOM
python3 requests.py 3
python3 log_analyser.py 
```


### Ports 
```
5000 - requests.py ->  master.py  
4000 - master.py   ->  worker.py 4000 1  
4001 - master.py   ->  worker.py 4001 2  
4002 - master.py   ->  worker.py 4002 3
5001 - worker.py   ->  master.py
```  
