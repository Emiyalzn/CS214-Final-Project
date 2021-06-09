from os import terminal_size
import random
import json
import math
import numpy as np
from tqdm import tqdm
from population import init_population


class Gene:
    def __init__(self, data):
        """
        data: the data we use to compute the fittness
        gene: the gene list
        length: length of gene
        fittness: fittness of this individual
        """
        self.data = data
        self.gene = self.data2gene(data)
        self.length = len(self.gene)
        self.fittness = 0

    def __lt__(self, other):
        return self.fittness < other.fittness

    def data2gene(self, data):
        """
        This function transforms the data used in DAG to gene
        """
        ans = []
        for i in data:
            ans += i
        return ans


class GA:
    def __init__(self, func=None, cfg=None, loader=None):
        """
        func: The function that computes the complete time of candidate
        cfg: The configuration
        """
        if not loader == None:
            self.loader = loader
        if not cfg == None:
            self.cfg = cfg
            self.func = func
            self.data_list = init_population(
                loader, cfg.GA.POPULATION)
            self.popu = [Gene(data) for data in self.data_list]

    def gene2data(self, gene):
        """
        This function transforms gene to the data we use in DAG
        task_num: list containing number of tasks of each job
        """
        ans = []
        pos = 0
        task_num = self.loader.task_num
        for n in task_num:
            ans.append(gene[pos:pos+n])
            pos += n
        return ans

    def compute_fittness(self, individual):
        """
        This function evaluates the fittness time of this individual
        """
        value, task = self.func(self.loader, individual.data)
        return 1 / np.mean(list(task.values())) * 1 / value * 1000

    def evaluate_population(self):
        """
        This function evaluates the fittness time of the whole population
        """
        for individual in self.popu:
            individual.fittness = self.compute_fittness(individual)

    def crossover(self, gene_pair):
        """
        This function generate two offsprings of the pair of Gene
        pair: [gene1, gene2]
        mode: 1 or 2
        mode = 1 ===> single-point-crossover
        mode = 2 ===> double-point-crossover
        """
        length = gene_pair[0].length
        gene1, gene2 = gene_pair[0].gene, gene_pair[1].gene
        gene_new1, gene_new2 = [], []
        if self.cfg.GA.CROSSOVER.MODE == 2:
            pos1 = random.randrange(1, length)
            pos2 = random.randrange(1, length)
            for i in range(length):
                if min(pos1, pos2) <= i < max(pos1, pos2):
                    gene_new1.append(gene1[i])
                    gene_new2.append(gene2[i])
                else:
                    gene_new1.append(gene2[i])
                    gene_new2.append(gene1[i])
        # TODO: mode=1
        if random.random() < self.cfg.GA.MUTATION.PROBABILITY:
            gene_new1 = self.mutation(gene_new1)
            gene_new2 = self.mutation(gene_new2)
        data_new1, data_new2 = self.gene2data(
            gene_new1), self.gene2data(gene_new2)
        return [Gene(data_new1), Gene(data_new2)]

    def mutation(self, gene):
        """
        This function randomly changes one element in gene
        bound: the pair of lower and upper bound of mutation element
        """
        bound = [1, self.loader.DC_num]
        length = len(gene)
        pos = random.randrange(0, length)
        gene[pos] = random.randint(bound[0], bound[1])
        return gene

    def selection(self):
        """
        This function randomly selects k individuals from the population
        individuals with higher fittness are more likely to be chosen
        """
        s_popu = sorted(self.popu, reverse=True)
        sum_fittness = sum(p.fittness for p in self.popu)
        print("Mean fittness: {}".format(sum_fittness/len(self.popu)))
        chosen = []  # The chosen individuals
        k = self.cfg.GA.NEXT_GEN
        for i in range(k):
            threshold = random.random() * sum_fittness
            cur_sum_fittness = 0
            for individual in s_popu:
                cur_sum_fittness += individual.fittness
                if cur_sum_fittness >= threshold:
                    chosen.append(individual)
                    break
        return sorted(chosen, reverse=False)

    def select_best(self):
        """
        This function selects the best individual
        """
        return sorted(self.popu, reverse=True)[0]

    def prob_decay(self):
        # self.cfg.GA.CROSSOVER.PROBABILITY *= (1 - self.cfg.GA.PROB_DECAY)
        self.cfg.GA.MUTATION.PROBABILITY *= (1 - self.cfg.GA.PROB_DECAY)
        print("crossover prob: {:.4f}, mutation prob: {:.4f}".
              format(self.cfg.GA.CROSSOVER.PROBABILITY,
                     self.cfg.GA.MUTATION.PROBABILITY))

    def optimize(self):
        """
        The critical function of GA
        Selects a group and generates new generation
        """
        print("Optimization starts!")
        for gen in range(self.cfg.GA.MAX_GENERATION):
            self.evaluate_population()
            print('Generation: {}'.format(gen))
            # print("Best individual: gene = {}".format(self.select_best().gene))
            value, task = self.func(self.loader, self.select_best().data)
            print('total computation time: {}'.format(max(list(task.values()))))
            print('mean computation time: {}'.format(
                np.mean(list(task.values()))))
            chosen = self.selection()
            next_gen = []
            while len(next_gen) < self.cfg.GA.NEXT_GEN:
                pair = [chosen.pop() for _ in range(2)]
                if random.random() < self.cfg.GA.CROSSOVER.PROBABILITY:
                    new_pair = self.crossover(pair)
                    next_gen.append(new_pair[0])
                    next_gen.append(new_pair[1])
                else:
                    next_gen.extend(pair)
            self.popu = next_gen
            self.prob_decay()
