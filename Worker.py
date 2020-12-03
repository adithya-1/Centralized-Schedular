#!/usr/bin/python
#Importing neccessary libraries for computation
import sys
import json
import socket
import threading
import time
import datetime

#Getting the required port number and worker ID as arguments from command line
portNo=int(sys.argv[1])
workerId=int(sys.argv[2])

#Initializing locks
lock1=threading.Lock()
lock2=threading.Lock()
lock3=threading.Lock()


#Creating a class for Worker
class Worker:
    def __init__(self,portNo,workerId,noSlots):     #Initializing parameters for Worker object
        self.portNo=portNo
        self.workerId=workerId
        self.noSlots=noSlots
        self.avaSolts=noSlots
        self.slotJobs=dict()
        for i in range(1,self.noSlots+1):        #First Bool value specifies available slots, second parameter indicates duration and third parameter indicates task_id
            self.slotJobs[i]=[True,0,'']


#Loading data from the config file
with open('config.json') as f:
            data = json.load(f)

#Setting up TCP connection
class TCPServer:
#Initializing paramters for TCP socket
    def __init__(self,port):
        self.port = port
        self.ip = "localhost"
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.jobQueue=[]      #Creating a queue for received jobs

#Starting the TCP server
    def startserver(self):
        self.sock.bind((self.ip, self.port))
        self.sock.listen(1)
        while True:    #Listening to Master TCP to send job data
                print('waiting for a connection at ',self.port)
                connection, clientAddress = self.sock.accept()
                job=connection.recv(1024)     #Getting job data from TCP socket
                     #Appending the received job into the job queue
                
                print(f'port {self.port} receives {job}')      #Displaying job reveceived
               
                #Accessing job parameters from job data by loading into json
                message=job.decode('utf-8')
                message=json.loads(message)
                
                lock1.acquire()      #Consume a lock for the job
                workerClass.avaSolts-=1     #Consume a slot for the job
                while True:
                    count=0
                    for k,v in workerClass.slotJobs.items():      #Changing slot parameters of the worker when job gets assigned
                        if(v[0]):
                            v[0]=False      #Changing slot status for be not free
                            v[1]= int(message["duration"])
                            v[2]=message["task_id"] 
                            break 
                        else:
                            count+=1
                    #condition satisfied when woker slot gets assigned a job and breaks from while loop
                    if(count!=workerClass.noSlots):
                        break
                
                     
                
                lock1.release()     #Release lock after initiation of job
                connection.close()
                
                #Send the time since epoch for received task into a file for logs analysis
                fileWrite="received:"+str(message['task_id'])+","+str(datetime.datetime.now().timestamp() * 1000)+"\n"
                print(fileWrite)
                f.write(fileWrite)  
              
                    
# Sending a message back to Master using 5001
def send_request():
    
    while(True):
        #Changing slot variabels on completion of task
        for k,v in workerClass.slotJobs.items():
            #If duration is 0 i,e when task is completed
            
            if(v[1]==0 and v[2]!=''):
                jobCompleted=v[2]    #Add task to completed task list
                lock2.acquire()      #Consume another lock
                workerClass.avaSolts+=1      #Free the slot for the worker
                v[2]=''
                v[0]=True            #Set slot status as available
                 
                lock2.release()     #Release the lock
                #Append the time since epoch when the task is completed into a file for logs analysis
                fileWrite="completed:"+str(jobCompleted)+","+str(datetime.datetime.now().timestamp() * 1000)+"\n"
                f.write(fileWrite)
                #Send the above information to Master via TCP socket through port 5001
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                    s.connect(("localhost", 5001))
                    message={"worker_id":workerClass.workerId,"avaSlots":workerClass.avaSolts,"slotJobs":v,"slot_id":k,"jobCompleted":jobCompleted}
                       
                    print(f'sending {message} to {5001}') 
                    message=json.dumps(message)
                    
                    #sending the task
                    s.send(message.encode())
                    
            elif(v[1]>0):      #If task is not completed, then sleep for 1 second and decrease the duration for the task
                print(v)
                lock3.acquire()
                v[1]-=1
                lock3.release()
        
        time.sleep(1)
        
        
#Checking for matching parameters specified for workers using config file
workers=data['workers']
workerClass=None
for i in range(0,len(workers)):
    
    if(workers[i]['worker_id']==workerId and workers[i]['port']==portNo):
        workerClass=Worker(portNo,workerId,workers[i]['slots'])
        break

#If parameters not matched, error is thrown
if(workerClass==None):
    print('Sorry given worker id  does not match with respective port number.Please fill appropriate inputs to the program')
    exit
else:    #Closing the file which was creating for logs analysis
    fileName='workerLogs'+str(workerClass.portNo)+'.txt'
    
    
    f=open(fileName,"a+",buffering=1)
    
    serverWorker=TCPServer(workerClass.portNo)

#initiating threading for parallelism
    threads = [threading.Thread(target=serverWorker.startserver), threading.Thread(target=send_request)]
    

    for th in threads:
        th.start()
        print(f'threads {th} started')
        th.join(0.1)
    
    
                
        
        
        
        
        
        
        
    
