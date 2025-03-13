# Centralized Scheduling Framework

## Files Description

* **BD_YACS_doc.pdf**,**Copy of BD_YACS_doc.docx**,**Copy of BD_YACS_presentation.pptx** - Contains detailed information about the YACS problem statement and tasks given

1. **Link for Report**-https://docs.google.com/document/d/1LdATPdnXL5Vwj_SmwLPAih77Ky-NQ5vJBe7ebKyR5Z4/edit?usp=sharing
2. **requests.py** - Python code for sending requests to Master node containing configuration of the jobs and resource allocation
3. **config.json** - Contains the Worker node configuration(Slots alloted, Port number, ID)
4. **Master.py** - Python code for the Master node that allocates and distributes jobs to Worker nodes
5. **Worker.py** - Python code for the Worker Nodes that receives job and executes them for given duration and sends back message to Master on error or completion
6. **workerLogs4000.txt** - Logs file for Worker Node 4000
7. **workerLogs4001.txt** - Logs file for Worker Node 4001
8. **workerLogs4002.txt** - Logs file for Worker Node 4002
9. **masterJobLogs.txt** - Logs file for Master Node

10.**LL/RR/RANDOMLogs.txt** - Logs file for each scheduler algorithm

11. **analysisOne.py** - Python code to analyse mean and median of the Logs data and plot graphs
12. **analysisTwo.py** - Python code for analysis of tasks running per node for each scheduling algorithm along with time taken. Graphs plotted for the same

---

# Steps for Execution :

**Master Node**

```Master.py config.json _schedulingAlgo_```
**Worker Node**

```Worker.py _port#_ _WorkerID_```
**analysisOne**

```analysis.py _LogsFileName_```

**analysisTwo**

```analysisTwo.py _LL/RR/RANDOMLogs.txt_```
