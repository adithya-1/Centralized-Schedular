#!/usr/bin/python
import sys
import json
import socket
import threading
import time

portNo=int(sys.argv[1])
workerId=int(sys.argv[2])

lock1=threading.Lock()
lock2=threading.Lock()
lock3=threading.Lock()
class Worker:
    def __init__(self,portNo,workerId,noSlots):
        self.portNo=portNo
        self.workerId=workerId
        self.noSlots=noSlots
        self.avaSolts=noSlots
        self.slotJobs=dict()
        for i in range(1,self.noSlots+1):
            self.slotJobs[i]=[True,0,'']
            
        
        
    def useSlot(self):
        
        if(self.avaSolts==0):
            print('Error in 26')
        else:
            self.avaSolts-=self.avaSolts
            
    
    def releaseSlot(self):
        if(not (self.avaSolts>=self.noSlots)):
            self.avaSolts+=1
        else:
            print('Error in 35')
         
with open('config.json') as f:
            data = json.load(f)

class TCPServer:
    def __init__(self,port):
        self.port = port
        self.ip = "localhost"
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.jobQueue=[]

    def startserver(self):
        self.sock.bind((self.ip, self.port))
        self.sock.listen(1)
        while True:
                print('waiting for a connection at ',self.port)
                connection, clientAddress = self.sock.accept()
                job=connection.recv(1024)
                self.jobQueue.append(job)
                connection.close()
                print(f'port {self.port} receives {job}')
                individualJob=self.jobQueue.pop(0)
                
                message=individualJob.decode('utf-8')
                message=json.loads(message)
                lock1.acquire()
                workerClass.avaSolts-=1
                for k,v in workerClass.slotJobs.items():
                    if(v[0]):
                        v[0]=False
                        v[1]= int(message["duration"])
                        v[2]=message["task_id"] 
                        break  
                lock1.release()         
                    

def send_request():
    
    while(True):
        
        for k,v in workerClass.slotJobs.items():
            time.sleep(1)
            if(v[1]==0 and v[2]!=''):
                jobCompleted=v[2]
                lock2.acquire()
                workerClass.avaSolts+=1
                v[2]=''
                v[0]=True
                lock2.release()
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                    s.connect(("localhost", 5001))
                    message={"worker_id":workerClass.workerId,"avaSlots":workerClass.avaSolts,"slotJobs":v,"slot_id":k,"jobCompleted":jobCompleted}
                       
                    print(f'sending {message} to {5001}') 
                    message=json.dumps(message)
                    #send task
                    s.send(message.encode())
            elif(v[1]>0):
                
                lock3.acquire()
                v[1]-=1
                lock3.release()
         
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
    
    
                
        
        
        
        
        
        
        
    
