#!/usr/bin/python
import sys
import json
import socket
import random
import time
import threading

from numpy.lib.function_base import meshgrid

lock1=threading.Lock()
lock2=threading.Lock()
lock3=threading.Lock()
lock4=threading.Lock()
pathToConfig= sys.argv[1]
schType=sys.argv[2]

class Worker:
    def __init__(self,portNo,workerId,noSlots):
        self.portNo=portNo
        self.workerId=workerId
        self.noSlots=noSlots
        self.avaSlots=noSlots
        self.slotJobs=dict()
        for i in range(1,self.noSlots+1):
            self.slotJobs[i]=[True,0,'']
        
        

    
    def updateAva(self,ava):
        self.avaSlots=ava

with open(pathToConfig) as f:
        data = json.load(f)

workers=data['workers']
globalWorkers=dict()
for i in workers:
    globalWorkers[i['worker_id']]=Worker(i['port'],i['worker_id'],i['slots'])

def schedulingRandom():
    count=0
    solWorker=random.choice(list(globalWorkers.values()))
    while True:
        
        if(solWorker.avaSlots>0):
            return solWorker
        else:
            solWorker=random.choice(list(globalWorkers.values()))

def schedulingRound():
    count=0
    while True:
   
        
        temp=sorted (globalWorkers.keys())
        for k in temp:
        
            if(globalWorkers[k].avaSlots>0):
                return globalWorkers[k]

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
globalJobContent=dict()

class TCPServer:
    def __init__(self,port):
        self.port = port
        self.ip = "localhost"
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.bind((self.ip, self.port))
        self.sock.listen(1)
        self.jobQueue=[]

    def startserver(self):
          while True:
                print('waiting for a connection at ',self.port)
                connection, clientAddress = self.sock.accept()
                job=connection.recv(1024)
                self.jobQueue.append(job)
                print(f'port {self.port} receives {job}')
                connection.close()           
                individualJob=self.jobQueue.pop(0)
                
                
                if(self.port==5000):
                    
                    message=individualJob.decode('utf-8') 
                    message=json.loads(message)
                    globalJobContent[message['job_id']]=list()
                    subList=list()
                    for i in message['map_tasks']:
                        subList.append([i['task_id'],i['duration'],False])
                    globalJobContent[message['job_id']].append(subList)
                    subList=list()
                    for i in message['reduce_tasks']:
                        subList.append([i['task_id'],i['duration'],False])
                    globalJobContent[message['job_id']].append(subList)
                    
                    globalJobContent[message['job_id']].append(False)
                    
                    for i in globalJobContent[message['job_id']][0]:
                        tempWorker=None
                        if(schType=='RANDOM'):
                            tempWorker=schedulingRandom()
                        elif(schType=='RR'):
                            tempWorker=schedulingRound()
                        elif(schType=='LL'):
                            tempWorker=schedulingLeast()
                        
                        # tempWorker=globalWorkers[1]
                        
                        if(tempWorker!=None):
                        
                            send_request(i,tempWorker)
                        
                if(self.port==5001):
                    message=individualJob.decode('utf-8') 
                    message=json.loads(message)
                    lock2.acquire()
                    globalWorkers[message['worker_id']].avaSlots=message['avaSlots']
                    globalWorkers[message['worker_id']].slotJobs[message['slot_id']]=message['slotJobs']
                    lock2.release()
                    a=message['jobCompleted']
                    if(a[2]=='M'):
                      
                        for i in globalJobContent[a[0]][0]:
                            if(i[0]==message['jobCompleted']):
                                i[2]=True
                                break
                            
                        startReducer=False
                        for i in globalJobContent[a[0]][0]:
                            if(i[2]==False):
                                startReducer=False
                                break
                            else:
                                startReducer=True
                        print(startReducer)
                        if(startReducer):
                            globalJobContent[a[0]][2]=True
                        for k,v in globalJobContent.items():
                            if(v[2]):
                                if(len(v[1])>0):
                                    lock3.acquire()
                                    reducerJob=v[1].pop(0)
                                    lock3.release()
                                    tempWorker=None
                                    if(schType=='RANDOM'):
                                        tempWorker=schedulingRandom()
                                    elif(schType=='RR'):
                                        tempWorker=schedulingRound()
                                    elif(schType=='LL'):
                                        tempWorker=schedulingLeast()
                                    
                                    if(tempWorker!=None):
                                        send_request(reducerJob,tempWorker)
                                    break
                                    
                                    
                    
                    elif(a[2]=='R'):
                        for i in globalJobContent[a[0]][1]:
                            if(i[0]==message['jobCompleted']):
                                i[2]=True
                                break
                        for k,v in globalJobContent.items():
                            if(v[2]):
                                if(len(v[1])>0):
                                    lock4.acquire()
                                    reducerJob=v[1].pop(0)
                                    lock4.release()
                                    tempWorker=None
                                    if(schType=='RANDOM'):
                                        tempWorker=schedulingRandom()
                                    elif(schType=='RR'):
                                        tempWorker=schedulingRound()
                                    elif(schType=='LL'):
                                        tempWorker=schedulingLeast()
                                    
                                    if(tempWorker!=None):
                                        send_request(reducerJob,tempWorker)
                                    break
                        
                    else:
                        print('Error in 153')
                               
                    
                   
                    
                    
                    
                              
s5000=TCPServer(5000)
s5001=TCPServer(5001)


def send_request(job,worker):
    lock1.acquire()
    worker.avaSlots-=1
    x=job
    job={"task_id":x[0],"duration":x[1]}
    for k,v in worker.slotJobs.items():
        if(v[0]):
            v[0]=False
            v[1]=job["duration"]
            v[2]=job["task_id"]
            break
    lock1.release()
    
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect(("localhost", worker.portNo))
            
        message=json.dumps(job)
            
        print(f'sending {message} to {worker.portNo}')   
        s.send(message.encode())

threads = [threading.Thread(target=s5000.startserver), threading.Thread(target=s5001.startserver)]

for th in threads:
    th.start()
    print(f'threads {th} started')
    th.join(0.1)




            
            
        
    
    
    
            
    