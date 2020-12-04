
import matplotlib.pyplot as plt
import sys
import seaborn
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
# data=[x1,y1]

# heatmap=seaborn.heatmap(data)

plt.xlabel('time')
plt.ylabel('tasks')
plt.title(fileName)
plt.legend()
plt.show()

#similarly we can do it for other scheduling algorithms by reading task info from different file

