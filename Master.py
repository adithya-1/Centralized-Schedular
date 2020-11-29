#!/usr/bin/python
import sys
import json
import socket
import random
import time



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
                

sock5000 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
serverAddress5000=('localhost',5000)
sock5000.bind(serverAddress5000)
sock5000.listen(1)
while True:
    # Wait for a connection
    print('waiting for a connection at 5000')
    connection5000, client_address5000 = sock5000.accept()
    job=connection5000.recv(1024)
    clientSocket=socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    tempWorker=globalWorkers[1]
    print('AAAAAAAAAAAAAAAA')
    print(tempWorker.portNo)
    clientSocket.connect(("localhost",tempWorker.portNo)) 
    clientSocket.send(job)
       
    
    print(job)
    connection5000.close()





            
            
        
    
    
    
            
    