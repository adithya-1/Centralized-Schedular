#!/usr/bin/python

#importing required libraries
import sys
import json
import socket
import random
import time
import threading
import datetime

#initialising locks
lock1=threading.Lock()
lock2=threading.Lock()
lock3=threading.Lock()
lock4=threading.Lock()
lock5=threading.Lock()
lock6=threading.Lock()

#getting arguments from user
pathToConfig= sys.argv[1]
schType=sys.argv[2]

#definition of worker class
class Worker:
    def __init__(self,portNo,workerId,noSlots):
        #port no
        self.portNo=portNo 
        #workerId
        self.workerId=workerId
        #total number of slots
        self.noSlots=noSlots
        #total number of available slots
        self.avaSlots=noSlots
        #jobs running in each slot
        self.slotJobs=dict()
        #initialising slot jobs
        for i in range(1,self.noSlots+1):
            self.slotJobs[i]=[True,0,'']   #[slotAvailable,duration value,running task]

#loading configuration file
with open(pathToConfig) as f:
        data = json.load(f)
#getting list of workers
workers=data['workers']

#global workers dictionary
globalWorkers=dict()

#initialising global dictionary with portNo,workerId,Slots
for i in workers:
    globalWorkers[i['worker_id']]=Worker(i['port'],i['worker_id'],i['slots'])

#Random Scheduling Algorithm - gets a random woker and checks if it has avaliable slots,if slot exsist then return the worker else look randomly
#for another worker
def schedulingRandom():
    solWorker=random.choice(list(globalWorkers.values()))
    while True:
        lock5.acquire()
        if(solWorker.avaSlots>0):
            
            solWorker.avaSlots-=1
            lock5.release()
            return solWorker
        else:
            lock5.release()
            time.sleep(1)
            solWorker=random.choice(list(globalWorkers.values()))

#Round Robin Scheduling Algorithm - sorts the workers based on id and checks if slots are avaiable accordingly
def schedulingRound():
    while True:
        temp=sorted (globalWorkers.keys())
        for k in temp:
            
            # print("Available Slots= ",globalWorkers[k].avaSlots)
            lock5.acquire()
            if(globalWorkers[k].avaSlots>0):
                
                globalWorkers[k].avaSlots-=1
                lock5.release()
                
                return globalWorkers[k]
            else:
                lock5.release()
                # time.sleep(1)
        time.sleep(1)
#Least- Loaded Scheduling Algorithm - checks for the worker with most available slots.If no slots are available then waits for one second before checking again
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
            lock5.acquire()
            solWorker.avaSlots-=1
            lock5.release()
            return solWorker

#To check list of jobs to be processed
globalJobContent=dict()

#Class TCP Server
class TCPServer:
    def __init__(self,port):
        #port no
        self.port = port
        #ip 
        self.ip = "localhost"
        #socket connection
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        #bind to respective id and port
        self.sock.bind((self.ip, self.port))
        #listen to the port for incoming requests
        self.sock.listen(1)
        #queue of jobs for the server
        self.jobQueue=[]
       
    #definition to start the server
    def startserver(self):
        
        #keep the server running
        while True:
                print('waiting for a connection at ',self.port)
                #accept incoming requests
                connection, clientAddress = self.sock.accept()
                #receive the job
                job=connection.recv(1024)
                #append to job queue so that incoming jobs are not delayed
                self.jobQueue.append(job)
                print(f'port {self.port} receives {job} from {clientAddress}')
                #close the connection
                connection.close() 
                #decode the message     
                analyticMessage=job.decode('utf-8')
                #convert decoded message to dictionary
                analyticMessage=json.loads(analyticMessage)
               
               #logging recevied jobs
                try:
                    fileWrite="received:"+str(analyticMessage['job_id'])+","+str(datetime.datetime.now().timestamp() * 1000)+"\n"
                    f.write(fileWrite)
                    
                except:
                    #the last request will be empty
                    print('')
               
                 
                #poping the first item in job queue    
                individualJob=self.jobQueue.pop(0)
                
                
                #function to send jobs to respective workers
                if(self.port==5000):
                    
                    #decode the message
                    message=individualJob.decode('utf-8') 
                    #convert decoded message to dictionary
                    message=json.loads(message)
                    #storing jobs in global job list 
                    globalJobContent[message['job_id']]=list()
                    #first appending map tasks
                    subList=list()
                    for i in message['map_tasks']:
                        #taskId,duration,completedOrNot
                        subList.append([i['task_id'],i['duration'],False])
                    globalJobContent[message['job_id']].append(subList)
                    #second appending reducer tasks
                    subList=list()
                    for i in message['reduce_tasks']:
                        #taskId,duration,completedOrNot,sentOrNot
                        subList.append([i['task_id'],i['duration'],False,False])
                    globalJobContent[message['job_id']].append(subList)
                    
                    #boolean value to that signifies whether to start reducer or not
                    globalJobContent[message['job_id']].append(False)
                    #boolean value to that signifies whether reducer job is completed or not
                    globalJobContent[message['job_id']].append(False)
                    #finding worker based on scheduling alogrithm for map tasks
                    i=0
                    while(i<len(globalJobContent[message['job_id']][0])):
                  
                        tempWorker=None
                        if(schType=='RANDOM'):
                            tempWorker=schedulingRandom()
                        elif(schType=='RR'):
                            tempWorker=schedulingRound()
                        elif(schType=='LL'):
                            tempWorker=schedulingLeast()
                        
                        #after the worker is returned based on scheduling algorithm sending request to that worker
                        if(tempWorker!=None):
                            #checking again if that worker has available slots before sending
                            if(tempWorker.avaSlots>0):
                                
                                send_request(globalJobContent[message['job_id']][0][i],tempWorker)
                                i+=1
                            
                            
                                
                           
                
                #function to receive message from workers        
                if(self.port==5001):
                    #decode the message
                    message=individualJob.decode('utf-8') 
                    #convert decoded message to dictionary
                    message=json.loads(message)
                    #acquring locks to change available slots and slot jobs 
                    lock2.acquire()
                    globalWorkers[message['worker_id']].avaSlots=message['avaSlots']
                    globalWorkers[message['worker_id']].slotJobs[message['slot_id']]=message['slotJobs']
                    #releasing the jobs
                    lock2.release()
      
                    #seeing whch job got completed
                    a=message['jobCompleted']
                    #if the job is a mapper task
                    if(a.split('_')[1][0]=='M'):
                        
                        #updating that mapper task is completed in global job list
                        for i in globalJobContent[a.split('_')[0]][0]:
                            if(i[0]==message['jobCompleted']):
                                i[2]=True
                                break
                        
                        #checking to see if reducer function can start or not   
                        startReducer=False
                        for i in globalJobContent[a.split('_')[0]][0]:
                            if(i[2]==False):
                                startReducer=False
                                break
                            else:
                                startReducer=True
                                
                        #if reducer to be started
                        if(startReducer):
                            lock4.acquire()
                            #inidicating reducer functions have been started in global job list
                            globalJobContent[a.split('_')[0]][2]=True
                            lock4.release()
                            
                    
                            
                                    
                    #if message is a reducer
                    elif(a.split('_')[1][0]=='R'):
                        # updating global job list
                        for i in globalJobContent[a.split('_')[0]][1]:
                            if(i[0]==message['jobCompleted']):
                                i[2]=True
                                break
                        #checking if all  reducer jobs are completed
                        completeReducer=False
                        for i in globalJobContent[a.split('_')[0]][1]:
                            if(i[2]==False):
                                completeReducer=False
                                break
                            else:
                                completeReducer=True

                        if(completeReducer):
                            lock3.acquire()
                            #indicating reducer functions have been completed in the global list
                            globalJobContent[a.split('_')[0]][3]=True
                            lock3.release()
                            
                            
                      
                            fileWrite="completed:"+str(a.split('_')[0])+","+str(datetime.datetime.now().timestamp() * 1000)+"\n"
                            f.write(fileWrite)
                                   
                               
                               
                        
                    else:
                        #used for debugging purpose
                        print('Error in line 284')
                    
                     
                   

def startReducer():
    while True:
        #using try except since dictionary values keep changing
        try:
            for k,v in globalJobContent.items():
            #if all the reducer tasks are completed or not
                        if(not v[3]):
            #if for that particular job reducer is started
                            if(v[2]):
                                count=0
                                
                               
                                while(count<len(v[1])):
                                    #checking if reducer task was sent before or not
                                    if(not v[1][count][3]):
                                       
                                        reducerJob=v[1][count]
                                       

                                        #finding worker based on scheduling alogrithm for reduce tasks
                                        tempWorker=None
                                        if(schType=='RANDOM'):
                                            tempWorker=schedulingRandom()
                                        elif(schType=='RR'):
                                            tempWorker=schedulingRound()
                                        elif(schType=='LL'):
                                            tempWorker=schedulingLeast()
                                        
                                        if(tempWorker!=None):
                                            
                                            
                                            
                                            if(tempWorker.avaSlots>0):
                                                #changing value to true since task was sent
                                                v[1][count][3]=True
                                                
                                                send_request(reducerJob,tempWorker)
                                        break
                                        
                                    count+=1
        
        except:
            print('')                         
                                                   
                    
#initialising TCP server at 5000                             
s5000=TCPServer(5000)
#initialising TCP server at 5001
s5001=TCPServer(5001)

#send request function
def send_request(job,worker):
    #acquring locks to change slot jobs value
    lock1.acquire()
    
    #changing the message as per requirement
    x=job
    job={"task_id":x[0],"duration":x[1]}
    #updating the slots in each worker
    for k,v in worker.slotJobs.items():
        if(v[0]):
            v[0]=False
            v[1]=job["duration"]
            v[2]=job["task_id"]
            break
    lock1.release()
    
    #sending the message to the specified worker
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect(("localhost", worker.portNo))
            
        message=json.dumps(job)
            
        print(f'sending {message} to {worker.portNo}')   
        s.send(message.encode())

#opening log files to record job initial request time and job completion time        
f=open("masterJobLogs.txt","a+",buffering=1)
#log file tasks for each scheduling algorithm
fileOneName=str(schType)+"Logs.txt"
f1=open(fileOneName,"a+",buffering=1)

#checking total number of jobs running on each machine for every second
def analysisTwo():
    timeX=0
    while(True):
        
        for k,v in globalWorkers.items():
            writeContent="time:"+ str(timeX)+";"+ "worker_id:"+str(v.workerId)+";"+"jobs_running:"+str(v.noSlots-v.avaSlots)+'\n'
            f1.write(writeContent)
        timeX+=1
        time.sleep(1)
    
#initiating threading for parallelism
threads = [threading.Thread(target=s5000.startserver), threading.Thread(target=s5001.startserver),threading.Thread(target=analysisTwo),threading.Thread(target=startReducer)]


for th in threads:
    th.start()
    print(f'threads {th} started')
    th.join(0.1)




            
            
        
    
    
    
            
    