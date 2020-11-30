#!/usr/bin/python
import sys
import json
import socket
import threading
import time

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

class TCPServer:
    def __init__(self,port):
        self.port = port
        self.ip = "localhost"
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)


    def startserver(self):
        self.sock.bind((self.ip, self.port))
        self.sock.listen(1)
        while True:
                print('waiting for a connection at ',self.port)
                connection, clientAddress = self.sock.accept()
                job=connection.recv(1024)
                print(f'port {self.port} receives {job}')
                connection.close()
    

def send_request():
    while True:
    	with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect(("localhost", 5001))
            time.sleep(2)
            message='this is message from ' + str(workerClass.workerId)
            #send task
            s.send(message.encode())

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
    serverWorker=TCPServer(workerClass.portNo)
    threads = [threading.Thread(target=serverWorker.startserver), threading.Thread(target=send_request)]
    

    for th in threads:
        th.start()
        print(f'threads {th} started')
        th.join(0.1)
    
    
                
        
        
        
        
        
        
        
    
