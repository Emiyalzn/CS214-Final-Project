import json
from os import read


def read_json(json_path):
    return json.load(open(json_path, 'r'))


class data_loader:
    def __init__(self, cfg):
        # load data from json
        self.task_dict = read_json(cfg.DATA_PATH.task_dict)
        self.bandwidth = read_json(cfg.DATA_PATH.bandwidth)
        self.data_req = read_json(cfg.DATA_PATH.data_req)
        self.location = read_json(cfg.DATA_PATH.location)
        self.slot = read_json(cfg.DATA_PATH.slot)
        # compute data info
        self.DC_num = len(self.slot.keys())  # num of data center
        self.stage_num = max([len(job_info["stage"].keys())
                              for job_info in self.task_dict.values()])  # num of stage
        self._job2int()  # job ==> int
        self.calc_task()  # compute task num
        self.get_prev()

    def _job2int(self):
        self.job2int = {}
        index = 0
        for job in self.task_dict.keys():
            if not job in self.job2int.keys():
                self.job2int[job] = index
                index += 1

    def calc_task(self):
        self.task_num = []
        for job_dict in self.task_dict.values():
            self.task_num.append(len(job_dict['Execution Time'].keys()))

    def get_prev(self):
        self.prev = {}
        for job, job_dict in self.task_dict.items():
            for order in job_dict['order']:
                if not order[1] in self.prev.keys():
                    self.prev[order[1]] = []
                self.prev[order[1]].append(order[0])
