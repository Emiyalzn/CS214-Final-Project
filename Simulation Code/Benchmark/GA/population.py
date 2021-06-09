import random
import json
import numpy
import math
from tqdm import tqdm

from data_loader import read_json, data_loader

def init_population(loader, population_num = 1000):
    print("Population initialization starts!")
    population = [[[random.randint(1, loader.DC_num) for i in range(
        task)]for task in loader.task_num]for j in tqdm(range(population_num))]
    return population
