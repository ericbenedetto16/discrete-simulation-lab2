# CSC 754 - Lab 2
# Author: Eric Benedetto
import numpy as np
import queue
import random
from enum import Enum
import pandas as pd
from datetime import datetime
from matplotlib import pyplot as plt

class USER_STATUS(Enum): 
    THINKING = "thinking"
    QUEUED = "queued"
    ACTIVE = "active"

class SERVER_STATUS(Enum): 
    IDLE = "idle",
    BUSY = "busy"

# Constants
numUsers = 100
numSeconds = 18000
thinkTime = [18, 22]
responseTime = [9, 11]
queueingTimeScale = 0.975
processingTimeScale = 0.025

# Counters
metrics = list()
queueLenTime = list()
simClock = 0
numRequests = 0
completedRequests = 0

# State Variables
userQueue = queue.Queue()
server_status = SERVER_STATUS.IDLE
users = list()
currServingUID = -1
avgResponseTime = 0

# Util Functions
def generateThinkTime():
    return random.uniform(thinkTime[0], thinkTime[1])

def generateResponseTime():
    return random.uniform(responseTime[0], responseTime[1])

def calcQueueingTime(t):
    return queueingTimeScale * t

def calcProcessingTime(t):
    return processingTimeScale * t

# Class to Manage Requests
class Request:
    def __init__(self, receivedTime, thinkingTime):
        self.receivedTime = receivedTime
        self.thinkingTime = thinkingTime
        self.schedulingDelay = None
        self.queueingTime = None
        self.processingTime = None
        self.fulfilledTime = None

    def setQueueingTime(self, t):
        self.queueingTime = t

    def setProcessingTime(self, t):
        self.processingTime = t

    def setFulfilledTime(self, t):
        self.fulfilledTime = t

    def setSchedulingDelay(self, t):
        self.schedulingDelay = t

# Class for Managing Users
class User:
    def __init__(self, uid):
        self.uid = uid
        self.requests = list()
        self.numRequests = 0
        self.prevStatus = None
        self.status = USER_STATUS.THINKING
        self.thinkingTime = generateThinkTime()
        self.nextEvent = simClock + self.thinkingTime

    def arrive(self):
        self.requests.append(Request(self.nextEvent, self.thinkingTime))
        self.numRequests += 1
        
        responseTime = generateResponseTime()
        self.requests[self.numRequests - 1].setQueueingTime(calcQueueingTime(responseTime))
        self.requests[self.numRequests - 1].setProcessingTime(calcProcessingTime(responseTime))
        
        self.nextEvent = simClock + calcQueueingTime(responseTime)
    
    def arriveInQueue(self):

        self.requests.append(Request(self.nextEvent, self.thinkingTime))
        self.numRequests += 1

        self.prevStatus = self.status
        self.status = USER_STATUS.QUEUED

        responseTime = generateResponseTime()
        processingTime = calcProcessingTime(responseTime)
        queueingTime = calcQueueingTime(responseTime)

        dequeue = userQueue.queue
        if len(dequeue) > 0:
            lastQueued = dequeue[len(dequeue) - 1]
            lastQueuedFinishTime = lastQueued.nextEvent + lastQueued.requests[lastQueued.numRequests - 1].processingTime

            # Avoid Scheduling Conflicts
            if(lastQueuedFinishTime > simClock + queueingTime):
                newQueueingTime = lastQueuedFinishTime - simClock + 0.0001

                self.requests[self.numRequests - 1].setSchedulingDelay(newQueueingTime - queueingTime)
                queueingTime = newQueueingTime

        self.requests[self.numRequests - 1].setQueueingTime(queueingTime)
        self.requests[self.numRequests - 1].setProcessingTime(processingTime)
        
        self.nextEvent = simClock + queueingTime

    def getServed(self):
        self.prevStatus = self.status
        
        self.nextEvent = simClock + self.requests[self.numRequests - 1].processingTime
        
        self.status = USER_STATUS.ACTIVE

    def departServer(self):
        self.prevStatus = self.status

        self.requests[self.numRequests - 1].setFulfilledTime(simClock)

        self.status = USER_STATUS.THINKING
        
        self.thinkingTime = generateThinkTime()
        self.nextEvent = simClock + self.thinkingTime

# Util to Next User Who Will Finish Thinking
def getNextEventDetails():
    closestEventTime = numSeconds + 1
    closestEventIdx = -1

    for idx, user in enumerate(users):
        if user.nextEvent is not None and user.nextEvent < closestEventTime:
            closestEventTime = user.nextEvent
            closestEventIdx = idx

    return [closestEventTime, closestEventIdx]

# Init
for i in range(numUsers):
    users.append(User(i))

# Set up Logging
now = datetime.now()
queueLog = open(f"{now.strftime('%m-%d-%Y-%H%M%S')}_queue.log", mode="w")

# Simulation Loop
while simClock < numSeconds:
    [simClock, currServingUID] = getNextEventDetails()

    user = users[currServingUID]

    # User Arrives to Empty Queue, No Wait
    if user.status == USER_STATUS.THINKING and server_status is SERVER_STATUS.IDLE:
        if userQueue.qsize() > 0:
            user.arriveInQueue()
            userQueue.put(user)

            queueLog.write(f"({round(simClock, 2)}) User {currServingUID} Arrived In Queue. There are Currently {userQueue.qsize()} Users Waiting.\n")

            queueLenTime.append({'time': simClock, 'queueSize': userQueue.qsize()})
        else:
            user.arrive()
            queueLog.write(f"({round(simClock, 2)}) User {currServingUID} Arrived\n")

            user.getServed()
            queueLog.write(f"({round(simClock, 2)}) Processing User {currServingUID}'s Request\n")

            server_status = SERVER_STATUS.BUSY

    # User Arrives While Someone Else is Being Served, Add to Queue
    elif user.status == USER_STATUS.THINKING and server_status is SERVER_STATUS.BUSY:        
        user.arriveInQueue()
        userQueue.put(user)

        queueLog.write(f"({round(simClock, 2)}) User {currServingUID} Arrived In Queue. There are Currently {userQueue.qsize()} Users Waiting.\n")
        queueLenTime.append({'time': simClock, 'queueSize': userQueue.qsize()})

    # User is in the Process of Being Served, Complete their Request
    elif user.status is USER_STATUS.ACTIVE and server_status is SERVER_STATUS.BUSY:
        user.departServer()

        queueLog.write(f"({round(simClock, 2)}) Response Sent to User {currServingUID}\n")
        
        server_status = SERVER_STATUS.IDLE

    # User Cannot be Getting Served if Server is Idle, Error
    elif user.status == USER_STATUS.ACTIVE and server_status is SERVER_STATUS.IDLE:
        raise Exception("Should Not Be Active While Server is Idle")

    # User is Ready to Leave Queue and Enter Server, Make them Active
    elif user.status is USER_STATUS.QUEUED and server_status is SERVER_STATUS.IDLE:
        user = userQueue.get()

        queueLenTime.append({'time': simClock, 'queueSize': userQueue.qsize()})

        user.getServed()
        queueLog.write(f"({round(simClock, 2)}) Processing User {user.uid}'s Request from Queue\n")

        server_status = SERVER_STATUS.BUSY

    # User Cannot Leave Queue is Server is Already Busy, Error
    elif user.status is USER_STATUS.QUEUED and server_status is SERVER_STATUS.BUSY:
        raise Exception(f"Should Not Be Looking At Queue ({user.uid}) While Server is Busy")
    
    # Unhandled Condition, Error
    else:
        raise Exception(f"Unhandled Condition for {user.status} and {server_status}")

# Close File Handles
queueLog.close()

# Generate Reports
for user in users:
    numRequests += user.numRequests
    for request in user.requests:
        if request.fulfilledTime is not None:
            completedRequests += 1
        
        metrics.append({
            "user_id": user.uid, 
            "Received Time": request.receivedTime,
            "Queueing Time": request.queueingTime,
            "Scheduling Delay": request.schedulingDelay,
            "Processing Time": request.processingTime, 
            "Fulfilled": request.fulfilledTime,
            "Thinking Time": request.thinkingTime,
            "Response Time": request.queueingTime + request.processingTime,
            })

df = pd.DataFrame(metrics)

col = df["Queueing Time"]

T = numSeconds # Observation Time
A = numRequests # Number of Requests Arrived in T
C = completedRequests # Number of Completed Requests in T
arrivalRate = A / T # Arrival Rate
X = C / T # Throughput
B = np.sum(df["Processing Time"]) # Time Busy in T
U = B / T * 100 # Utilization % in T
S = B / C # Average Service Time Per Request in T

queueDf = pd.DataFrame(queueLenTime)
plt.title("Queue Length Over Time")
plt.plot(queueDf['time'], queueDf['queueSize'])
plt.show()

print(df)

print(f"Total Requests: {numRequests}")
print(f"Total Responses: {completedRequests}")

print("")

print(f"Average Response Time: {round(df['Response Time'].mean(), 2)}")
print(f"Average Thinking Time: {round(df['Thinking Time'].mean(), 2)}")
print(f"Average Queueing Time: {round(df['Queueing Time'].mean(), 2)}")
print(f"Average Processing Time: {round(df['Processing Time'].mean(), 2)}")

print("")

print(f"Arrival Rate: {round(arrivalRate, 4)} Requests/Second")
print(f"Throughput: {round(X, 4)} Responses/Second")
print(f"Busy Time: {round(B, 4)} Seconds")
print(f"Utilization: {round(U, 4)}%")
print(f"Average Service Time Per Request: {round(S, 4)} Seconds/Request")

print("")

print(f"Min Queueing Time {df['Queueing Time'].min()}")
print(f"Max Queueing Time {df['Queueing Time'].max()}")
print(f"Min Processing Time {df['Processing Time'].min()}")
print(f"Max Processing Time {df['Processing Time'].max()}")

print("")

print(f"Total Delay for Scheudling Conflicts in Queue {round(np.sum(df['Scheduling Delay']), 2)}")
print(f"Average Queueing Time Without Scheduling Conflicts {round((np.sum(df['Queueing Time']) - np.sum(df['Scheduling Delay'])) / numRequests, 2)}")
print(f"Average Response Time Without Scheduling Conflicts {round((np.sum(df['Response Time']) - np.sum(df['Scheduling Delay'])) / numRequests, 2)}")
