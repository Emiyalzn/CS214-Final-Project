# -*- coding: utf-8 -*-
import json
import heapq
import copy
import random
import time

globalTasks = {}
globalDataCenters = {}
nameToCenterNum = {}
globalBandWidth = {}
globalDataLocation = {}
DEBUG = False

tmpJudge = None

class Task():
    def __init__(self, taskName, jobName, exeTime):
        self.taskName = taskName
        self.jobName = jobName
        self.exeTime = exeTime
        self.stage = -1
        self.indegree = 0
        self.outdegreeNameList = []
        self.ready = True
        self.dataRequirement = {}
        self.endTime = 0
        self.slot = None
    def __lt__(self, other):
        return self.endTime < other.endTime
    def calcEndTime(self, currentTime, dataLocation):
        transferTime = 0.0
        for source, dataVolumn in self.dataRequirement.items():
            t = dataVolumn / bandWidthBetween(dataLocation[source], self.slot)
            transferTime = max(transferTime, t)
        return currentTime + transferTime + self.exeTime
    def finishTask(self, currentTime, tasks, dataLocation):
        if DEBUG:
            print(f"{self.taskName} finished at {currentTime}")
        dataLocation[self.taskName] = self.slot
        for outTaskName in self.outdegreeNameList:
            t = tasks[outTaskName]
            t.indegree -= 1
            if t.indegree == 0:
                t.ready = True
            elif t.indegree < 0:
                raise ValueError("degree error")

class Event():
    """
    事件类，描述某个时间所发生的事件
    """
    def __init__(self, time, func, param1, param2):
        self.time = time
        self.func = func
        self.param1, self.param2 = param1, param2
    def execute(self):
        self.func(self.time , self.param1, self.param2)
    def __lt__(self, other):
        return self.time < other.time

class DataCenter():
    def __init__(self, name, numOfSlot):
        self.name = name
        self.numOfSlot = numOfSlot
        self.slotHeap = []
        self.taskNameList = []
        self.nowTaskIndex = 0
        self.finished = False
    def loadTaskNameListFromString(self, taskNameListStr, tasks):
        name, taskNameListStr = taskNameListStr.split(':')
        name = name.strip()
        if name != self.name:
            return False
        else:
            self.taskNameList = []
            taskNameList = taskNameListStr.split('->')
            taskNameList.pop()
            for taskName in taskNameList:
                self.addTask(tasks[taskName.strip()])
            return True

    def swapTwoTasks(self, index1, index2):
        self.taskNameList[index1], self.taskNameList[index2] = \
        self.taskNameList[index2], self.taskNameList[index1]
        
    def addTask(self, task):
        task.slot = self.name
        self.taskNameList.append(task.taskName)

    def existReady(self, tasks) -> (bool, int):
        for idx in range(self.nowTaskIndex, len(self.taskNameList)):
            if tasks[self.taskNameList[idx]].ready:
                return (True, idx)
        return (False, len(self.taskNameList))

    def runTask(self, currentTime, eventHeap, tasks, dataLocation) -> bool:
        # 如果slotHeap装满了
        if len(self.slotHeap) >= self.numOfSlot:
            if self.slotHeap[0].endTime > currentTime:
                return False
            else:
                heapq.heappop(self.slotHeap)
        # 如果任务跑完了
        if self.nowTaskIndex >= len(self.taskNameList):
            self.finished = True
            return False
        nowTaskName = self.taskNameList[self.nowTaskIndex]
        nowTask = tasks[nowTaskName]
        if nowTask.ready:
            nowTask.ready = False
            if DEBUG:
                print(f"{nowTask.taskName} run at {currentTime}")
            nowTask.endTime = nowTask.calcEndTime(currentTime, dataLocation)
            # 注册结束事件
            heapq.heappush(eventHeap, Event(nowTask.endTime, nowTask.finishTask, tasks, dataLocation))
            heapq.heappush(self.slotHeap, nowTask)
            self.nowTaskIndex += 1
            return True
        else:
            return False
    def __lt__(self, other):
        return self.name < other.name
    def __str__(self):
        returnStr = self.name + " : "
        for taskName in self.taskNameList:
            returnStr += taskName + " -> "
        # returnStr += f'\nindex = {self.nowTaskIndex}'
        return returnStr

"""
Simulation函数组：
@globalDataCenters: dict    DataCenter name -> DataCenter类
@nameToCenterNum: dict      DataCenter name -> DataCenter在bandwidth矩阵的下标
@globalTasks: dict          task name -> Task类
@globalBandWidth: [[],[]]         DataCenter下标 -> 两个DataCenter之间的带宽
@globalDataLocation: dict   data name -> data所在的DataCenter
@eventHeap: list            事件堆
"""
def init(slotFile, band, location, dataDict, dataReq):
    """
    Simulation模拟器类构造函数，输入五个参数
    @slotFile:      slot.json文件名
    @globalBandWidth:     经过Floyd扩展后的带宽矩阵json文件名
    @location:      datacenter中包含哪些资源，json文件名
    """
    # =====globalDataCenters=====
    global globalDataCenters
    for dataCenterName, slotNum in json.load(open(slotFile, 'r')).items():
        globalDataCenters[dataCenterName] = DataCenter(dataCenterName, slotNum)
    # =====nameToCenterNum=====
    global nameToCenterNum
    for idx, name in enumerate(globalDataCenters.keys()):
        nameToCenterNum[name] = idx
    # =====globalTasks=====
    global globalTasks
    for jobName, jobDict in json.load(open(dataDict, 'r')).items():
        for taskName, exeTime in jobDict["Execution Time"].items():
            globalTasks[taskName] = Task(taskName, jobName, exeTime)
        stageDict = jobDict["stage"]
        for stage, taskNameList in enumerate(stageDict.values()):
            for taskName in taskNameList:
                globalTasks[taskName].stage = stage
        orderList = jobDict["order"]
        for edge in orderList:
            nameA, nameB = edge[0], edge[1]
            globalTasks[nameA].outdegreeNameList.append(nameB)
            globalTasks[nameB].indegree += 1
            globalTasks[nameB].ready = False
    for taskName, dataInfoDict in json.load(open(dataReq, 'r')).items():
        globalTasks[taskName].dataRequirement = dataInfoDict
    # =====globalBandWidth=====
    global globalBandWidth
    globalBandWidth = json.load(open(band, 'r'))
    # =====globalDataLocation=====
    global globalDataLocation
    globalDataLocation = json.load(open(location, 'r'))

def bandWidthBetween(name1, name2):
    global globalBandWidth, nameToCenterNum
    return globalBandWidth[nameToCenterNum[name1]]\
        [nameToCenterNum[name2]]

def run(dna) -> (bool, float):
    dna.refresh()
    dataCenterList = dna.dataCenterList
    eventHeap = []
    currentTime = 0.0
    while True:
        executed = False
        # dna.displayTasks()
        for dataCenter in dataCenterList:
            executed = dataCenter.runTask(currentTime, eventHeap, dna.tasks, dna.dataLocation) or executed
        if not executed:
            if len(eventHeap) == 0:
                finished = True
                for dataCenter in dataCenterList:
                    if not dataCenter.finished:
                        finished = False
                        break
                if finished:
                    return (True, currentTime)
                else:
                    return (False, float('inf'))
            event = heapq.heappop(eventHeap)
            event.execute()
            currentTime = event.time

def randomlyGenerateDataCenterList(tasks)->list:
    """返回一个引用tasks的dataCenterList"""
    global globalDataCenters
    taskNameList = copy.deepcopy(list(tasks.keys()))
    random.shuffle(taskNameList)
    dataCenterList = copy.deepcopy(list(globalDataCenters.values()))
    for taskName in taskNameList:
        dataCenterList[random.randint(0, len(dataCenterList)-1)]\
            .addTask(tasks[taskName])
    return dataCenterList

class DNA():
    def __init__(self, fileName=None):
        global globalTasks, globalDataLocation
        self.dataLocation = copy.deepcopy(globalDataLocation)
        self.tasks = copy.deepcopy(globalTasks)
        self.dataCenterList = randomlyGenerateDataCenterList(self.tasks)
        if fileName is not None:
            f = open(fileName, 'r')
            succ = True
            for idx, line in enumerate(f):
                succ = succ and self.dataCenterList[idx].loadTaskNameListFromString(line, self.tasks)
            f.close()
            if not succ:
                raise ValueError('load failed')

    def __str__(self):
        returnStr = ""
        for dataCenter in self.dataCenterList:
            returnStr += dataCenter.__str__() + '\n'
        return returnStr
    def save(self, fileName):
        f = open(fileName, 'w')
        f.write(self.__str__())
        f.close()
    def saveToDYM(self, fileName):
        f = open(fileName, 'w')
        ansList = []
        for task in self.tasks.values():
            ansList.append(task.slot)
        f.write(str(ansList))
        f.close()

    def refresh(self):
        global globalDataLocation
        self.dataLocation = copy.deepcopy(globalDataLocation)
        for task in self.tasks.values():
            task.indegree = 0
            task.ready = True
            task.endTime = 0
        for task in self.tasks.values():
            for outTaskName in task.outdegreeNameList:
                self.tasks[outTaskName].indegree += 1
                self.tasks[outTaskName].ready = False
        for dataCenter in self.dataCenterList:
            dataCenter.slotHeap = []
            dataCenter.nowTaskIndex = 0
            dataCenter.finished = False

    # def swapTwoDataCenter(self, firstIndex, secondIndex):
    #     self.dataCenterList[firstIndex].taskNameList, \
    #     self.dataCenterList[secondIndex].taskNameList = \
    #     self.dataCenterList[secondIndex].taskNameList, \
    #     self.dataCenterList[firstIndex].taskNameList

    # def sendLoadToAnother(self, senderIndex, sendeeIndex):
    #     # sender send 1/3 workload to sendee
    #     senderList = self.dataCenterList[senderIndex].taskNameList
    #     sendeeList = self.dataCenterList[sendeeIndex].taskNameList
    #     length, num = len(senderList), len(senderList)//3
    #     appendList = senderList[length - num : length]
    #     for _ in range(num):
    #         senderList.pop()
    #     sendeeList.extend(appendList)

    def sendOneTaskToAnother(self, senderIndex, sendeeIndex):
        senderList = self.dataCenterList[senderIndex].taskNameList
        sendeeList = self.dataCenterList[sendeeIndex].taskNameList
        taskName = senderList.pop()
        sendeeList.append(taskName)
        self.tasks[taskName].slot = self.dataCenterList[sendeeIndex].name

    def innerOptimize(self) -> (bool, float):
        self.refresh()
        eventHeap = []
        currentTime = 0.0
        while True:
            executed = False
            for dataCenter in self.dataCenterList:
                result = dataCenter.runTask(currentTime, eventHeap, self.tasks, self.dataLocation)
                if not result:
                    exist, idx = dataCenter.existReady(self.tasks)
                    if exist:
                        dataCenter.swapTwoTasks(dataCenter.nowTaskIndex, idx)
                        result = dataCenter.runTask(currentTime, eventHeap, self.tasks, self.dataLocation)
                executed = result or executed
            if not executed:
                if len(eventHeap) == 0:
                    finished = True
                    for dataCenter in self.dataCenterList:
                        if not dataCenter.finished:
                            finished = False
                            break
                    if finished:
                        return (True, currentTime)
                    else:
                        return (False, float('inf'))
                event = heapq.heappop(eventHeap)
                event.execute()
                currentTime = event.time

    def optimizeLoad(self):
        minVal = 0x7fffffff
        maxVal = -1
        maxIndex, minIndex = 0, 0
        for idx in range(len(self.dataCenterList)):
            num = len(self.dataCenterList[idx].taskNameList)
            if num > maxVal:
                maxVal = num
                maxIndex = idx
            if num < minVal:
                minVal = num
                minIndex = idx
        self.sendOneTaskToAnother(maxIndex, minIndex)
    def displayTasks(self):
        print('======================begin====================')
        for task in self.tasks.values():
            print(f"{task.taskName}.indegree = {task.indegree}, ready = {task.ready}")
        print('===============================================')
dir = "."
slotFile = f"./{dir}/slot.json"
band = f"./{dir}/bandwidth.json"
location = f"./{dir}/location.json"
dataDict = f"./{dir}/sorted_ToyData_dict.json"
dataReq = f"./{dir}/data_req.json"
init(slotFile, band, location, dataDict, dataReq)

def main():
    minTime = float('inf')
    start_time = time.time()
    i = 0
    while (i < 1000):
        oneDNA = DNA()
        succ, currentTime = oneDNA.innerOptimize()
        if succ and currentTime < minTime:
            print(f'bestTime = {currentTime}')
            minTime = currentTime
            oneDNA.save('best.dna')
        oneDNA.optimizeLoad()
        succ, currentTime = oneDNA.innerOptimize()
        if succ and currentTime < minTime:
            print(f'bestTime = {currentTime}')
            minTime = currentTime
            oneDNA.save('best.dna')
        oneDNA.optimizeLoad()
        succ, currentTime = oneDNA.innerOptimize()
        if succ and currentTime < minTime:
            print(f'bestTime = {currentTime}')
            minTime = currentTime
            oneDNA.save('best.dna')
        oneDNA.optimizeLoad()
        succ, currentTime = oneDNA.innerOptimize()
        if succ and currentTime < minTime:
            print(f'bestTime = {currentTime}')
            minTime = currentTime
            oneDNA.save('best.dna')
        i += 1
    end_time = time.time()
    print(end_time - start_time)
        
def testBestDNA():
    bestDNA = DNA('best.dna')
    # bestDNA.saveToDYM('best.dym')
    succ, runTime = run(bestDNA)
    print(f"succ = {succ}, runTime = {runTime}")
main()
# testBestDNA()
