#!/usr/bin/python
import sys
import json
import socket
portNo=int(sys.argv[1])
workerId=int(sys.argv[2])

class Worker:
    def __init__(self,portNo,workerId,noSlots):
        self.portNo=portNo
        self.workerId=workerId
        self.noSlots=noSlots
        self.avaSolts=noSlots
        
        
    def useSlot(self):
        if(self.avaSolts==0):
            return -1
        else:
            self.avaSolts-=self.avaSolts
            return 0
    
    def releaseSlot(self):
        if(not (self.avaSolts>=self.noSlots)):
            self.avaSolts+=1
        else:
            return -2
         
with open('config.json') as f:
            data = json.load(f)


workers=data['workers']
workerClass=None
for i in range(0,len(workers)):
    
    if(workers[i]['worker_id']==workerId and workers[i]['port']==portNo):
        workerClass=Worker(portNo,workerId,workers[i]['slots'])
        break


if(workerClass==None):
    print('Sorry given worker id  does not match with respective port number.Please fill appropriate inputs to the program')
    exit
else:
    
    sock= socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    
    serverAddress=("localhost",workerClass.portNo)
    
    sock.bind(serverAddress)
    sock.listen(1)
    
    while True:
        print('waiting for a connection at ',workerClass.portNo)
        connection, clientAddress = sock.accept()
        job=connection.recv(1024)
        print(job)
        connection.close()
        # if(job==1):
        #     success=workerClass.useSlot()
        #     if(success==0):
        #         connection.send(workerClass.avaSolts)
        #     else:
        #         connection.send('No available slots')
        #     connection.close()
    
                
        
        
        
        
        
        
        
    
