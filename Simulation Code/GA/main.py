from data_loader import data_loader
import json
import math
from GA import Gene, GA
from easydict import EasyDict as edict
from DAG import str2index, compute_running_time
import time


def get_config(cfg_path):
    return edict(json.load(open(cfg_path, 'r')))

if __name__ == '__main__':
    cfg = get_config('./config.json')
    loader = data_loader(cfg)
    ga = GA(compute_running_time, cfg, loader)
    start_time = time.time()
    ga.optimize()
    end_time = time.time()
    print(end_time - start_time)
