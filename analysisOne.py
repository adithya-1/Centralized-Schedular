import csv
import statistics
from csv import reader
import sys
import matplotlib.pyplot as plt

fileName=sys.argv[1]
#Creating 2 lists for storing all tasks and completion times of each task for the Workers
time_list=[]
task_list=[]
analysisDict=dict()
#Opening the preprocessed file which contains task-name,Completion-time

f = open(fileName,"r")
for line in f:
    line=line.strip("\n")
    line = line.split(",")
    task = line[0].split(':')[1]
    time = float(line[1])
    if(task not in analysisDict.keys()):
        analysisDict[task]=list()
        analysisDict[task].append(time)
    else:
        try:
            if(analysisDict[task][1]<time):
                analysisDict[task][1]=time
        except:
            analysisDict[task].append(time)
        

for k ,v in analysisDict.items():
    analysisDict[k]=abs(v[1]-v[0])/1000
    time_list.append(analysisDict[k])
    task_list.append(k)

#Calcualting Mean and Median for completion time for the Worker
sum =0

#Getting sum of all time
for t in time_list:
    sum = sum + t

#Generating Mean 
mean_time = sum/len(time_list)
print(mean_time)

#Generating Median
median_time = statistics.median(time_list)
print(median_time)

plt.plot(task_list,time_list)       #X-axis = job_list; Y-axis = time_list
plt.xlabel('Jobs')
plt.ylabel('Completion Time')
plt.title('Job Completion TIme')
plt.show()  
        

