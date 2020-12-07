# Importing neccessary libraries for computation and visualisation

import csv

import statistics

from csv import reader

import sys

import matplotlib.pyplot as plt



# Taking the file name as input from command line

fileName=sys.argv[1]
analysisQuestion=sys.argv[2]


if(int(analysisQuestion)==1):
    #Creating 2 lists for storing all tasks and completion times of each task for the Workers

    time_list=[]

    task_list=[]

    analysisDict=dict()



    #Opening the preprocessed file which contains task-name,time since epoch

    f = open(fileName,"r")

    for line in f:

        line=line.strip("\n")     #Stripping the lines off escpe sequence

        line = line.split(",")    #Splitting the line based on ,

        task = line[0].split(':')[1]     #Accessing the task name from each line

        time = float(line[1])            #Accessing time since epoch of each task

        if(task not in analysisDict.keys()):        #Creating a list to calculate completion time = completed task time - time of receiving the task

            analysisDict[task]=list()

            analysisDict[task].append(time)

        else:

            try:

                if(analysisDict[task][1]<time):      #Taking las possible completion time for a task if there exists a duplicate completion

                    analysisDict[task][1]=time

            except:

                analysisDict[task].append(time)

            

    #Subtracting received time from completion time of task to get final completion time of each task

    for k ,v in analysisDict.items():
        
        

        analysisDict[k]=abs(v[1]-v[0])/1000

        time_list.append(analysisDict[k])     #Appending the completion time into list

        task_list.append(k)                 #Appending the task name into task_list



    #Calcualting Mean and Median for completion time for the Worker

    sum =0



    #Getting sum of all time

    for t in time_list:

        sum = sum + t



    #Generating Mean 

    mean_time = sum/len(time_list)

    print(mean_time)



    #Generating Median using statistics library

    median_time = statistics.median(time_list)

    print(median_time)



    #PLotting a graph of Task_List vs Completion Time using pyplot

    plt.plot(task_list,time_list)       #X-axis = Task_list; Y-axis = time_list

    plt.xlabel('Tasks')

    plt.ylabel('Completion Time')

    plt.title('Task Completion TIme')



    #Displaying the graph

    plt.show()  
elif(int(analysisQuestion)==2):
    x1=[]
    y1=[]
    y2=[]
    y3=[]
    fileName=sys.argv[1]	#getting fileName from user

    f=open(fileName,"r")     #open the file having the task info. for each algorithm. Here it is LL scheduling.
    for line in f:
        line=line.split(";")
        time=((line[0].split(":"))[1])
        time=int(time)
        if(time not in x1):
            x1.append(time)    #append time to x1
        w=(line[1].split(":"))[1]  #get the worker id
        t=(line[2].split(":"))[1]  #get the no of tasks scheduled in that particular worker at a particular time
        w=int(w)
        t=int(t)
        if(w==1):
            y1.append(t)       #if worker id is 1 append to y1
        elif(w==2):
            y2.append(t)       #if worker id is 2 append to y2
        else:
            y3.append(t)       #if worker id is 3 append to y3


    #plot y1,y2,y3 versus time x1, this gives plt of no of tasks scheduled in a machine, against time. 		
    plt.plot(x1,y1,label='worker1')
    plt.plot(x1,y2,label='worker2')
    plt.plot(x1,y3,label='worker3')
  

    plt.xlabel('time')
    plt.ylabel('tasks')
    plt.title(fileName)
    plt.legend()
    plt.show()

    #similarly we can do it for other scheduling algorithms by reading task info from different file


        



