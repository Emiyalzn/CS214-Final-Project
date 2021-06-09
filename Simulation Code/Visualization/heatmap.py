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


def get_config(cfg_path):
    return edict(json.load(open(cfg_path, 'r')))


class visualization:
    def __init__(self, data, fig_name, loader) -> None:
        self.heatmap = data["heatmap"]
        self.fig_name = fig_name

    def draw_heatmap(self):
        # 作图阶段
        fig = plt.figure()
        # 定义画布为1*1个划分，并在第1个位置上进行作图
        ax = fig.add_subplot(111)
        # 定义横纵坐标的刻度
        ax.set_yticks(range(loader.DC_num))
        ax.set_yticklabels([str(i) for i in range(1, loader.DC_num+1)],fontsize = 10)
        ax.set_xticks(range(sum(loader.task_num)))
        ax.set_xticklabels([str(i) for i in range(1, sum(loader.task_num)+1)],fontsize = 6)
        # 作图并选择热图的颜色填充风格，这里选择hot
        im = ax.imshow(self.heatmap, cmap=plt.cm.hot_r)
        # 增加右侧的颜色刻度条
        plt.colorbar(im)
        # 增加标题
        plt.title(self.fig_name)
        plt.xlabel("Gene")
        plt.ylabel("Data Center")
        # save
        plt.savefig('./vis_result/{}.jpg'.format(self.fig_name))
        plt.close('all')
    


if __name__ == "__main__":
    cfg = get_config('./config.json')
    loader = data_loader(cfg)
    index = 0
    for root, dirs, files in os.walk('./result'):
        for file in tqdm(files):
            if index % 5 == 0:
                path = os.path.join(root, file)
                data = read_json(path)
                v = visualization(
                    data=data, fig_name="Generation {}".format('%04d'%index), loader=loader)
                v.draw_heatmap()
            index += 1
    images = []
    filenames = sorted(fn for fn in os.listdir('./vis_result/') )
    for filename in filenames:
        images.append(imageio.imread('./vis_result/'+filename))
    imageio.mimsave('./vis_result/gif.gif', images, duration=0.015)