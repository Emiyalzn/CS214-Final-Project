import sys
import json
from tqdm import tqdm
import numpy as np
from data_loader import read_json, data_loader
from population import init_population


def str2index(task):
    return int("".join(list(filter(str.isdigit, task))))


def compute_running_time(loader, gene=None):
    prev = loader.prev
    task_dict = loader.task_dict
    req_dict = loader.data_req
    loc_dict = loader.location
    bandwidth_dict = loader.bandwidth
    ans = {}
    # print(loader.stage_num)
    for stage in range(1, loader.stage_num+1):
        for job, job_dict in task_dict.items():
            stage_dict = job_dict['stage']
            exe_time_dict = job_dict["Execution Time"]
            job_index = loader.job2int[job]
            if str(stage) in stage_dict.keys():
                tasks = stage_dict[str(stage)]
                for task in tasks:
                    task_loc = gene[job_index][str2index(task)-1]
                    ans[task] = exe_time_dict[task] + (max([ans[p] for p in prev[task]]
                                                           ) if task in prev.keys() else 0)
                    transfer_time = 0
                    req = req_dict[task]
                    for loc, amount in req.items():  # transfer data
                        if loc[0] == 't':  # task
                            f = gene[job_index][str2index(loc) - 1]
                        else:
                            f = str2index(loc_dict[loc])
                        # print(f,task_loc)
                        bandwidth = bandwidth_dict[f - 1][task_loc - 1]
                        if not bandwidth == 0:
                            cur_transfer_time = amount/bandwidth
                            transfer_time = cur_transfer_time if cur_transfer_time > transfer_time else transfer_time
                        else:
                            print('ERROR')
                            return -1
                    ans[task] += transfer_time
    return max(ans.values()), ans
