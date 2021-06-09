import random
from matplotlib import pyplot as plt
from matplotlib import cm
from matplotlib import axes
from matplotlib.font_manager import FontProperties
import os
from tqdm import tqdm
import json
from data_loader import read_json, data_loader
from easydict import EasyDict as edict
import imageio
import numpy as np


res={}
res['best']=[]
res['mean']=[]
res['gen']=[]
index=0
for root, dirs, files in os.walk('./result'):
        for file in files:
            path = os.path.join(root, file)
            data = read_json(path)
            res['best'].append(data['best_fitness'])
            res['mean'].append(data['mean_fitness'])
            res['gen'].append(index)
            index += 1
plt.plot(res['gen'],res['best'],"bo-",linewidth=0.5,markersize=0.5,label="Best individual")
plt.plot(res['gen'],res['mean'],"gs-",linewidth=0.5,markersize=0.5,label="Average")
plt.xlabel("Generation")
plt.ylabel("Fitness")
plt.legend(loc="right")
plt.savefig("fitness.png")
plt.show()