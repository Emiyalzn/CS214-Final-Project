import json
import random
def intToStr(x) -> str:
    base = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ'
    ans = ''
    while x > 0:
        ans += base[x%26]
        x //= 26
    if len(ans) == 0:
        return 'A'
    else:
        return ans[::-1]
class Job():
    def __init__(self, jobName):
        self.jobName = jobName
        self.order = []
        self.executionTime = {}
        self.stage = {}
        self.dataReq = {}
        self.dataNum = random.randint(6, 8)
        curTask = 1
        stageNum = random.randint(1, 4)
        for curStage in range(1, stageNum+1):
            preLastTask = curTask - 1
            stageTask = random.randint(1, 3) # 这个stage有几个task
            stageNameList = []
            
            for _ in range(stageTask):
                curTaskName = f"t{jobName}{str(curTask)}"
                exeTime = random.randint(1, 100)/10
                self.executionTime[curTaskName] = exeTime
                curTask += 1
                stageNameList.append(curTaskName)
                edgeNum = random.randint(0, min(3, preLastTask))
                dataReqNum = random.randint(1, 5)
                dataReqIdxList = []
                taskDataReq = {}
                for _ in range(dataReqNum):
                    dataReqIdx = random.randint(1, self.dataNum)
                    if dataReqIdx in dataReqIdxList:
                        dataReqIdx = random.randint(1, self.dataNum)
                    dataReqIdxList.append(dataReqIdx)
                for dataReqIdx in dataReqIdxList:
                    dataName = f"{jobName}{dataReqIdx}"
                    taskDataReq[dataName] = 10* random.randint(1, 50)
                preTaskList = []
                # print(f"edgeNum = {edgeNum}")
                for _ in range(edgeNum):
                    preTaskIdx = random.randint(1, preLastTask)
                    while preTaskIdx in preTaskList:
                        preTaskIdx = random.randint(1, preLastTask)
                    preTaskList.append(preTaskIdx)
                for preTaskIdx in preTaskList:
                    preTaskName = f"t{jobName}{str(preTaskIdx)}"
                    self.order.append([preTaskName, curTaskName])
                    taskDataReq[preTaskName] = 10* random.randint(1,50)
                self.dataReq[curTaskName] = taskDataReq
            self.stage[str(curStage)] = stageNameList
            
    def __dict__(self):
        return {"order":self.order,"Execution Time":self.executionTime,"stage":self.stage}
def makeDataCenter(numOfDataCenter, maxOfSlot):
    dc = {}
    for i in range(1, numOfDataCenter+1):
        dc["DC%d"%i] = random.randint(1, maxOfSlot)
    with open("slot.json", 'w') as f:
        json.dump(dc, f)
    bandWidth = []
    for i in range(1, numOfDataCenter+1):
        lst = []
        for j in range(1, numOfDataCenter+1):
            if i == j:
                lst.append(1200)
            else:
                lst.append(10*random.randint(8, 50))
        bandWidth.append(lst)
    with open('bandwidth.json','w') as f:
        json.dump(bandWidth, f)
def makeTasks(numOfJobs, numOfDataCenter):
    locationDict = {}
    sortedToyDataDict = {}
    dataReqDict = {}
    for jobIdx in range(numOfJobs):
        jobName = intToStr(jobIdx)
        job = Job(jobName)
        dataNum = job.dataNum
        for dataIdx in range(1, 1+dataNum):
            dataName = jobName + str(dataIdx)
            datacenterName = "DC%d"%random.randint(1, numOfDataCenter)
            locationDict[dataName] = datacenterName
        sortedToyDataDict[jobName] = job.__dict__()
        dataReqDict.update(job.dataReq)
    with open('location.json', 'w') as f:
        json.dump(locationDict, f)
    with open('sorted_ToyData_dict.json', 'w') as f:
        json.dump(sortedToyDataDict, f)
    with open('data_req.json', 'w') as f:
        json.dump(dataReqDict, f)
def main():
    numOfJobs = 10
    numOfDataCenter = 6
    maxofSlot = 8
    makeDataCenter(numOfDataCenter, maxofSlot)
    makeTasks(numOfJobs, numOfDataCenter)
main()