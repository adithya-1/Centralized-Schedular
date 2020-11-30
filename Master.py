#!/usr/bin/python
import sys
import json
import socket
import random
import time
import threading

from numpy.lib.function_base import meshgrid


pathToConfig= sys.argv[1]
schType=sys.argv[2]

class Worker:
    def __init__(self,portNo,workerId,noSlots):
        self.portNo=portNo
        self.workerId=workerId
        self.noSlots=noSlots
        self.avaSlots=noSlots
        
        

    
    def updateAva(self,ava):
        self.avaSlots=ava

with open(pathToConfig) as f:
        data = json.load(f)

workers=data['workers']
globalWorkers=dict()
for i in workers:
    globalWorkers[i['worker_id']]=Worker(i['port'],i['worker_id'],i['slots'])

def schedulingRandom():
    solWorker=random.choice(list(globalWorkers.values()))
    while True:
        if(solWorker.avaSlots>0):
            return solWorker
        else:
            solWorker=random.choice(list(globalWorkers.values()))

def schedulingRound():
    temp=sorted (globalWorkers.keys())
    for k,v in temp.items():
        if(v.avaSlots>0):
            return v

def schedulingLeast():
    temp=list(globalWorkers.values())
    while True:
        maxAva=temp[0].avaSlots
        solWorker=temp[0]
        for i in temp[1:]:
            if(i.avaSlots>maxAva):
                maxAva=i.avaSlots
                solWorker=i
        if(maxAva==0):
            time.sleep(1)
        else:
            return solWorker

class TCPServer:
    def __init__(self,port):
        self.port = port
        self.ip = "localhost"
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.bind((self.ip, self.port))
        self.sock.listen(1)

    def startserver(self):
          while True:
                print('waiting for a connection at ',self.port)
                connection, clientAddress = self.sock.accept()
                job=connection.recv(1024)
                print(f'port {self.port} receives {job}')
                connection.close()
                if(self.port==5000):
                    tempWorker=globalWorkers[1]
                    send_request(job,tempWorker.portNo)
                              
s5000=TCPServer(5000)
s5001=TCPServer(5001)


def send_request(job,portno):
    while True:
    	with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect(("localhost", portno))
            message=job.decode('utf-8') 
            message=json.dumps(message)
            
            s.send(message.encode())

threads = [threading.Thread(target=s5000.startserver), threading.Thread(target=s5001.startserver)]

for th in threads:
    th.start()
    print(f'threads {th} started')
    th.join(0.1)




            
            
        
    
    
    
            
    