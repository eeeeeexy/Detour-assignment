import math, time, copy, json
from re import T
from math import sin, asin, cos, radians, fabs, sqrt
from itertools import permutations
import numpy as np
from collections import defaultdict

from utils import get_distance_hav
from utils import worker_seg_traj_distance
from utils import dist_group_traj
from utils import dist_task_traj
from utils import group_travel_cost
from utils import detour_pair

from utils import run_kuhn_munkres
from utils import Graph
from utils import real_distance, real_distance_group

def data_read(file1, file2, file3):
    # read task data
    with open(file1, 'r') as f_task:
        task_dict_1 = json.load(f_task)
    TaskPoint = {}
    for k in task_dict_1.keys():
        # if int(k) == 3001:
        #     break
        TaskPoint[int(k)] = task_dict_1[k]
    # print('task dict:', TaskPoint[1])

    # read location data
    with open(file2, 'r') as f_location:
        LocationTrajectory = json.load(f_location)
    LocationTrajectoryPoint = {}
    for k in LocationTrajectory.keys():
        LocationTrajectoryPoint[int(k)] = LocationTrajectory[k]
    # print('location dict:', LocationTrajectoryPoint[1])

    # read worker data
    with open(file3, 'r') as f_worker:
        worker_dict = json.load(f_worker)
    WorkerTrajectory = {}
    for k in worker_dict.keys():
        # if int(k) == 501:
        #     break
        WorkerTrajectory[int(k)] = worker_dict[k]
    # print('worker dict:', WorkerTrajectory[1])

    return TaskPoint, LocationTrajectoryPoint, WorkerTrajectory

def min_way(t, wj):
    min_dis = []
    min_dis.append(get_distance_hav(TaskPoint[t]['location'], LocationTrajectoryPoint[WorkerTrajectory[wj]['Trajectory'][0]]))

    for oi in range(1, len(WorkerTrajectory[wj]['Trajectory'])):

        diss = 0
        for ss in range(0, oi - 1):
            diss += get_distance_hav(LocationTrajectoryPoint[WorkerTrajectory[wj]['Trajectory'][ss]],
                                     LocationTrajectoryPoint[WorkerTrajectory[wj]['Trajectory'][ss + 1]])
        diss += get_distance_hav(TaskPoint[t]['location'], LocationTrajectoryPoint[WorkerTrajectory[wj]['Trajectory'][oi]])

        min_dis.append(diss)

    return min(min_dis)

def trajectorydistance(i, WorkerTrajectory, LocationTrajectoryPoint):
    dis = 0
    for j in range(0, len(WorkerTrajectory[i]['Trajectory'])-1):
        l1 = WorkerTrajectory[i]['Trajectory'][j]
        l2 = WorkerTrajectory[i]['Trajectory'][j+1]
        dis += get_distance_hav(LocationTrajectoryPoint[l1], LocationTrajectoryPoint[l2])
    return dis

def dist_task_schedule(task_schedule_circle, end_task):
    task_schedule = copy.deepcopy(task_schedule_circle)
    task_schedule.append(task_schedule_circle[0])
    dist = 0
    for ss in range(0, len(task_schedule) - 1):
        if ss == end_task:
            continue
        else:
            dist += get_distance_hav(TaskPoint[task_schedule[ss] + 1]['location'],
                                  TaskPoint[task_schedule[ss + 1] + 1]['location'])

    return dist


class Graph(object):
    def __init__(self, maps):
        self.maps = maps
        self.nodenum = self.get_nodenum()
        self.edgenum = self.get_edgenum()
        self.node = self.node

    def node(self):
        return {"id:": 0, "loc:": [0, 0], "tag:": 0}

    def get_nodenum(self):
        return len(self.maps)

    def get_edgenum(self):
        count = 0
        for i in range(self.nodenum):
            for j in range(i):
                if self.maps[i][j] > 0 and self.maps[i][j] < 9999:
                    # print(i, j, maps[i][j])
                    count += 1
        return count

    def prim(self):
        res = []
        if self.nodenum <= 0 or self.edgenum < self.nodenum - 1:
            return res
        seleted_node = [0]
        candidate_node = [i for i in range(1, self.nodenum)]
        while len(candidate_node) > 0:
            begin, end, minweight = 0, 0, 9999
            for i in seleted_node:
                for j in candidate_node:
                    if self.maps[i][j] < minweight:
                        minweight = self.maps[i][j]
                        begin = i
                        end = j
            res.append([begin, end, minweight])
            seleted_node.append(end)
            candidate_node.remove(end)

        # print('res:', len(res), res)
        nodes = []
        graph_dict = {}
        for i in range(0, len(res)):
            nodes.append(res[i][0])
            nodes.append(res[i][1])

        nodes = sorted(list(set(nodes)))

        for i in nodes:
            graph_dict[i] = {}

        for i in range(0, len(res)):
            if res[i][0] in graph_dict.keys():
                graph_dict[res[i][0]][res[i][1]] = res[i][2]
            if res[i][1] in graph_dict.keys():
                graph_dict[res[i][1]][res[i][0]] = res[i][2]
            else:
                graph_dict[res[i][0]] = {res[i][1]: res[i][2]}
                graph_dict[res[i][1]] = {res[i][0]: res[i][2]}

        return graph_dict

    def baseline_group(self, k, size):
        Group = []
        seleted_node = [i for i in range(0, self.nodenum)]
        candidate_node = [i for i in range(0, self.nodenum)]
        while len(candidate_node) > 0:
            for i in seleted_node:
                if i in candidate_node:
                    group_i = [i]
                    p = k
                    while p > 1:
                        minweight = 999
                        min_node = -1
                        for j in candidate_node:
                            if i != j and self.maps[i][j] < size and self.maps[i][j] < minweight:
                                minweight = self.maps[i][j]
                                min_node = j
                        if min_node != -1:
                            group_i.append(min_node)
                            candidate_node.remove(min_node)
                        p -= 1
                    candidate_node.remove(i)
                    Group.append(group_i)
        return Group

zero_threshold = 0.00000001

class KMNode(object):
    def __init__(self, id, exception=0, match=None, visit=False):
        self.id = id
        self.exception = exception
        self.match = match
        self.visit = visit

class KuhnMunkres(object):
    def __init__(self):
        self.matrix = None
        self.x_nodes = []
        self.y_nodes = []
        self.minz = float('inf')
        self.x_length = 0
        self.y_length = 0
        self.index_x = 0
        self.index_y = 1

    def __del__(self):
        pass

    def set_matrix(self, x_y_values):
        xs = set()
        ys = set()
        for x, y, value in x_y_values:
            xs.add(x)
            ys.add(y)

        if len(xs) < len(ys):
            self.index_x = 0
            self.index_y = 1
        else:
            self.index_x = 1
            self.index_y = 0
            xs, ys = ys, xs

        x_dic = {x: i for i, x in enumerate(xs)}
        y_dic = {y: j for j, y in enumerate(ys)}
        self.x_nodes = [KMNode(x) for x in xs]
        self.y_nodes = [KMNode(y) for y in ys]
        self.x_length = len(xs)
        self.y_length = len(ys)

        self.matrix = np.zeros((self.x_length, self.y_length))
        for row in x_y_values:
            x = row[self.index_x]
            y = row[self.index_y]
            value = row[2]
            x_index = x_dic[x]
            y_index = y_dic[y]
            self.matrix[x_index, y_index] = value

        for i in range(self.x_length):
            self.x_nodes[i].exception = max(self.matrix[i, :])

    def km(self):
        for i in range(self.x_length):
            while True:
                self.minz = float('inf')
                self.set_false(self.x_nodes)
                self.set_false(self.y_nodes)

                if self.dfs(i):
                    break

                self.change_exception(self.x_nodes, -self.minz)
                self.change_exception(self.y_nodes, self.minz)

    def dfs(self, i):
        x_node = self.x_nodes[i]
        x_node.visit = True
        for j in range(self.y_length):
            y_node = self.y_nodes[j]
            if not y_node.visit:
                t = x_node.exception + y_node.exception - self.matrix[i][j]
                if abs(t) < zero_threshold:
                    y_node.visit = True
                    if y_node.match is None or self.dfs(y_node.match):
                        x_node.match = j
                        y_node.match = i
                        return True
                else:
                    if t >= zero_threshold:
                        self.minz = min(self.minz, t)
        return False

    def set_false(self, nodes):
        for node in nodes:
            node.visit = False

    def change_exception(self, nodes, change):
        for node in nodes:
            if node.visit:
                node.exception += change

    def get_connect_result(self):
        ret = []
        for i in range(self.x_length):
            x_node = self.x_nodes[i]
            j = x_node.match
            y_node = self.y_nodes[j]
            x_id = x_node.id
            y_id = y_node.id
            value = self.matrix[i][j]

            if self.index_x == 1 and self.index_y == 0:
                x_id, y_id = y_id, x_id
            ret.append((x_id, y_id, value))

        return ret

    def get_max_value_result(self):
        ret = 0
        for i in range(self.x_length):
            j = self.x_nodes[i].match
            ret += self.matrix[i][j]

        return ret

def run_kuhn_munkres(x_y_values):
    process = KuhnMunkres()
    process.set_matrix(x_y_values)
    process.km()
    return process.get_max_value_result(), process.get_connect_result()

def MST(group_list, trajectory_list):

    flag = {}

    location_list = []
    for s in group_list:
        flag[s] = 0
        location_list.append(TaskPoint[s + 1]['location'])
    for j in trajectory_list:
        flag[j] = 1
        location_list.append(LocationTrajectoryPoint[j])

    maps = []
    for row in range(0, len(group_list) + len(trajectory_list)):
        row_i = []
        for low in range(0, len(group_list) + len(trajectory_list)):
            dis = get_distance_hav(location_list[row], location_list[low])
            row_i.append(dis)
        maps.append(row_i)

    graph = Graph(maps)
    spanning_tree_copy = graph.prim()
    spanning_tree = {}
    for i in spanning_tree_copy.keys():

        if i < len(group_list):
            spanning_tree[group_list[i]] = {}
        else:
            spanning_tree[trajectory_list[i - len(group_list)]] = {}

        for j in spanning_tree_copy[i].keys():

            if i < len(group_list) and j < len(group_list):
                spanning_tree[group_list[i]][group_list[j]] = spanning_tree_copy[i][j]
            elif i < len(group_list) and j >= len(group_list):
                spanning_tree[group_list[i]][trajectory_list[j - len(group_list)]] = spanning_tree_copy[i][j]
            elif i >= len(group_list) and j < len(group_list):
                spanning_tree[trajectory_list[i - len(group_list)]][group_list[j]] = spanning_tree_copy[i][j]
            elif i >= len(group_list) and j >= len(group_list):
                spanning_tree[trajectory_list[i - len(group_list)]][trajectory_list[j - len(group_list)]] = \
                spanning_tree_copy[i][j]

    # print(spanning_tree)
    return spanning_tree, flag

def dist_task_traj(task, traj_list):
    min_dist = 999
    min_traj = -1
    loc_g = TaskPoint[task + 1]['location']

    for j in traj_list:
        loc_t = LocationTrajectoryPoint[j]
        dist_task_trajectory = get_distance_hav(loc_g, loc_t)
        if dist_task_trajectory < min_dist:
            min_dist = dist_task_trajectory
            min_traj = j

    return min_traj, min_dist

def DFS(spanning_tree, flag, s):

    direct_graph = []

    stack = []
    stack.append(s)
    seen = set()
    seen.add(s)

    while (len(stack) > 0):
        vertex = stack.pop()
        nodes = spanning_tree[vertex]
        for w in nodes:
            if w not in seen:
                stack.append(w)
                seen.add(w)

        if flag[vertex] == 0 and vertex not in direct_graph:
            direct_graph.append(vertex)

    return direct_graph


def task_deadline_check(group_list, j, best_pair, worker_speed):
    # group_list : []
    # wc : j
    # best_pair : [start, end]

    flag = True

    prefix_dist = worker_seg_traj_distance(j, 0, WorkerTrajectory[j]['Trajectory'].index(best_pair[0]), WorkerTrajectory, LocationTrajectoryPoint)

    if (prefix_dist + get_distance_hav(LocationTrajectoryPoint[best_pair[0]], TaskPoint[group_list[0] + 1]['location'])) / worker_speed < TaskPoint[group_list[0] + 1]['expiration']:
        flag = False
    
    dist_task = 0
    for task in range(len(group_list)-1):
        start_task = group_list[task]
        next_task = group_list[task + 1]
        dist_task += get_distance_hav(TaskPoint[start_task + 1]['location'], TaskPoint[next_task + 1]['location'])
        if (prefix_dist + dist_task) / worker_speed < TaskPoint[next_task + 1]['expiration']:
            flag = False
            break
    
    return flag


def schedule_pair(j, group_list):

    tao = detour_rate * WorkerTrajectory[j]['distance']

    optimal_detour = []
    optimal_schedule = []
    final_optimal_cost = 0

    if len(group_list) > 1:

        spanning_tree, flag_tree = MST(group_list, WorkerTrajectory[j]['Trajectory'])
        task_schedule_circle = DFS(spanning_tree, flag_tree, group_list[0])

        optimal_cost = 99
        optimal_start_task = -1
        optimal_detour = []

        near_dist = 99
        for t in task_schedule_circle:
            for o in range(0, len(WorkerTrajectory[j]['Trajectory'])):
                dist_n = get_distance_hav(LocationTrajectoryPoint[WorkerTrajectory[j]['Trajectory'][o]],
                                       TaskPoint[t + 1]['location'])
                if dist_n < near_dist:
                    near_dist = dist_n

        for s in range(1, len(task_schedule_circle)):

            start_task = task_schedule_circle[s]
            end_task = task_schedule_circle[s - 1]

            dist_2 = dist_task_schedule(task_schedule_circle, end_task)

            candidate_traj = []
            for o in range(0, len(WorkerTrajectory[j]['Trajectory'])):

                if get_distance_hav(LocationTrajectoryPoint[WorkerTrajectory[j]['Trajectory'][o]],
                                 TaskPoint[start_task + 1]['location']) <= tao - near_dist - dist_2 or get_distance_hav(
                    LocationTrajectoryPoint[WorkerTrajectory[j]['Trajectory'][o]],
                    TaskPoint[end_task + 1]['location']) <= tao - near_dist - dist_2:
                    candidate_traj.append(o)

            best_detour_pair = []
            min_cost = 99
            for l in range(0, len(candidate_traj)):
                for r in range(len(candidate_traj) - 1, l, -1):
                    dist_5 = get_distance_hav(
                        LocationTrajectoryPoint[WorkerTrajectory[j]['Trajectory'][candidate_traj[l]]],
                        TaskPoint[start_task + 1]['location']) + get_distance_hav(
                        LocationTrajectoryPoint[WorkerTrajectory[j]['Trajectory'][candidate_traj[r]]],
                        TaskPoint[end_task + 1]['location']) + dist_2
                    if dist_5 < tao and task_deadline_check(group_list, j, [WorkerTrajectory[j]['Trajectory'][candidate_traj[l]],
                                            WorkerTrajectory[j]['Trajectory'][candidate_traj[r]]], worker_speed) == True:
                        # print('pair:', l, r, dist_5, dist_5 - worker_seg_traj_distance(j, l, r))
                        best_detour_pair = [WorkerTrajectory[j]['Trajectory'][candidate_traj[l]],
                                            WorkerTrajectory[j]['Trajectory'][candidate_traj[r]]]
                        min_cost = dist_5 - worker_seg_traj_distance(j, candidate_traj[l], candidate_traj[r], WorkerTrajectory, LocationTrajectoryPoint)
                        break
                break
            if min_cost < optimal_cost and min_cost > 0:
                optimal_cost = min_cost
                optimal_detour = best_detour_pair
                optimal_start_task = start_task

        if optimal_start_task != -1:

            left_list = []
            right_list = []
            for i in range(0, len(task_schedule_circle)):
                if task_schedule_circle.index(task_schedule_circle[i]) >= task_schedule_circle.index(
                        optimal_start_task):
                    right_list.append(task_schedule_circle[i])
                else:
                    left_list.append(task_schedule_circle[i])
            right_list.extend(left_list)

            final_optimal_cost = optimal_cost
            optimal_detour = optimal_detour
            optimal_schedule = right_list

    if len(group_list) == 1:
        task_center = group_list[0]

        near_dist = 99

        for oo in range(0, len(WorkerTrajectory[j]['Trajectory'])):

            dist_4 = get_distance_hav(LocationTrajectoryPoint[WorkerTrajectory[j]['Trajectory'][oo]],
                                   TaskPoint[task_center + 1]['location'])
            if dist_4 < near_dist:
                near_dist = dist_4

        candidate_traj = []
        for o in range(0, len(WorkerTrajectory[j]['Trajectory'])):
            if get_distance_hav(LocationTrajectoryPoint[WorkerTrajectory[j]['Trajectory'][o]],
                             TaskPoint[task_center + 1]['location']) < tao - near_dist:
                candidate_traj.append(o)

        best_detour_pair = []
        optimal_cost = -1
        for l in range(0, len(candidate_traj)):
            for r in range(len(candidate_traj) - 1, l, -1):
                dist_5 = get_distance_hav(
                    LocationTrajectoryPoint[WorkerTrajectory[j]['Trajectory'][candidate_traj[l]]],
                    TaskPoint[task_center + 1]['location']) + get_distance_hav(
                    LocationTrajectoryPoint[WorkerTrajectory[j]['Trajectory'][candidate_traj[r]]],
                    TaskPoint[task_center + 1]['location'])
                if dist_5 < tao and task_deadline_check(group_list, j, [WorkerTrajectory[j]['Trajectory'][candidate_traj[l]],
                                            WorkerTrajectory[j]['Trajectory'][candidate_traj[r]]], worker_speed) == True:
                    best_detour_pair = [WorkerTrajectory[j]['Trajectory'][candidate_traj[l]],
                                        WorkerTrajectory[j]['Trajectory'][candidate_traj[r]]]
                    optimal_cost = dist_5 - worker_seg_traj_distance(j, candidate_traj[l], candidate_traj[r], WorkerTrajectory, LocationTrajectoryPoint)
                    break
            break
        if len(candidate_traj) > 1 and optimal_cost > 0:

            final_optimal_cost = optimal_cost
            optimal_detour = best_detour_pair
            optimal_schedule = [task_center]

    return optimal_schedule, final_optimal_cost, optimal_detour

def flag_l(flag_list):
    flag = True
    for i in flag_list:
        if i == 1:
            flag = False
    return flag

def real_distance(schedule, assignment):

    total_cost = 0

    for pair in assignment:

        if pair[0] in schedule.keys() and pair[1] in schedule[pair[0]].keys():
            l_d = schedule[pair[0]][pair[1]]['detour'][0]
            l_c = schedule[pair[0]][pair[1]]['detour'][1]
            index_d = WorkerTrajectory[pair[0]]['Trajectory'].index(l_d)
            index_c = WorkerTrajectory[pair[0]]['Trajectory'].index(l_c)

            schedule_list = schedule[pair[0]][pair[1]]['schedule']

            real_task = 0
            if len(schedule_list) > 1:
                for s in range(0, len(schedule_list) - 1):
                    real_task += get_distance_hav(TaskPoint[schedule_list[s] + 1]['location'],
                                                  TaskPoint[schedule_list[s + 1] + 1]['location'])
            real_task += get_distance_hav(LocationTrajectoryPoint[l_d], TaskPoint[schedule_list[0] + 1]['location'
            ]) + get_distance_hav(LocationTrajectoryPoint[l_c], TaskPoint[schedule_list[-1] + 1]['location'])

            real_dis = 0
            for p in range(index_d, index_c):
                real_dis += get_distance_hav(LocationTrajectoryPoint[p], LocationTrajectoryPoint[p + 1])
            total_cost += real_task - real_dis

        elif pair[0] in schedule.keys():
            single_schedule, single_cost, single_detour = schedule_pair(pair[0], greedy_group[pair[1]])
            if len(single_schedule) == 0:
                total_cost += 0

            else:
                schedule[pair[0]][pair[1]] = {}
                schedule[pair[0]][pair[1]]['detour'] = single_detour
                schedule[pair[0]][pair[1]]['schedule'] = single_schedule

                l_d = schedule[pair[0]][pair[1]]['detour'][0]
                l_c = schedule[pair[0]][pair[1]]['detour'][1]
                index_d = WorkerTrajectory[pair[0]]['Trajectory'].index(l_d)
                index_c = WorkerTrajectory[pair[0]]['Trajectory'].index(l_c)
                schedule_list = schedule[pair[0]][pair[1]]['schedule']

                real_task = 0
                if len(schedule_list) > 1:
                    for s in range(0, len(schedule_list) - 1):
                        real_task += get_distance_hav(TaskPoint[schedule_list[s] + 1]['location'],
                                                      TaskPoint[schedule_list[s + 1] + 1]['location'])
                real_task += get_distance_hav(LocationTrajectoryPoint[l_d], TaskPoint[schedule_list[0] + 1]['location'
                ]) + get_distance_hav(LocationTrajectoryPoint[l_c], TaskPoint[schedule_list[-1] + 1]['location'])

                real_dis = 0
                for p in range(index_d, index_c):
                    real_dis += get_distance_hav(LocationTrajectoryPoint[p], LocationTrajectoryPoint[p + 1])
                total_cost += real_task - real_dis

    return total_cost

def real_distance_group(schedule, wi, group_t):

    if wi in schedule.keys() and group_t in schedule[wi].keys():

        l_d = schedule[wi][group_t]['detour'][0]
        l_c = schedule[wi][group_t]['detour'][1]
        index_d = WorkerTrajectory[wi]['Trajectory'].index(l_d)
        index_c = WorkerTrajectory[wi]['Trajectory'].index(l_c)
        schedule_list = schedule[wi][group_t]['schedule']

        real_task = 0
        if len(schedule_list) > 1:
            for s in range(0, len(schedule_list) - 1):
                real_task += get_distance_hav(TaskPoint[schedule_list[s] + 1]['location'],
                                              TaskPoint[schedule_list[s + 1] + 1]['location'])
        real_task += get_distance_hav(LocationTrajectoryPoint[l_d], TaskPoint[schedule_list[0] + 1]['location'
                    ]) + get_distance_hav(LocationTrajectoryPoint[l_c], TaskPoint[schedule_list[-1] + 1]['location'])

        real_dis = 0
        for p in range(index_d, index_c):
            real_dis += get_distance_hav(LocationTrajectoryPoint[p], LocationTrajectoryPoint[p + 1])

        # schedule[wi][group_t]['cost'] = real_task - real_dis
        return real_task - real_dis

    # elif wi in schedule.keys() and group_t in schedule[wi].keys() and len(schedule[wi][group_t]['schedule']) == 0:
    #     return -1000
    else:
        single_schedule, single_cost, single_detour = schedule_pair(wi, greedy_group[group_t])
        if len(single_schedule) == 0:
            # schedule[wi][group_t]['cost'] = -1000
            return -1000
        else:
            schedule[wi][group_t] = {}
            schedule[wi][group_t]['detour'] = single_detour
            schedule[wi][group_t]['schedule'] = single_schedule

            l_d = schedule[wi][group_t]['detour'][0]
            l_c = schedule[wi][group_t]['detour'][1]
            index_d = WorkerTrajectory[wi]['Trajectory'].index(l_d)
            index_c = WorkerTrajectory[wi]['Trajectory'].index(l_c)
            schedule_list = schedule[wi][group_t]['schedule']

            real_task = 0
            if len(schedule_list) > 1:
                for s in range(0, len(schedule_list) - 1):
                    real_task += get_distance_hav(TaskPoint[schedule_list[s] + 1]['location'],
                                                  TaskPoint[schedule_list[s + 1] + 1]['location'])
            real_task += get_distance_hav(LocationTrajectoryPoint[l_d], TaskPoint[schedule_list[0] + 1]['location'
            ]) + get_distance_hav(LocationTrajectoryPoint[l_c], TaskPoint[schedule_list[-1] + 1]['location'])

            real_dis = 0
            for p in range(index_d, index_c):
                real_dis += get_distance_hav(LocationTrajectoryPoint[p], LocationTrajectoryPoint[p + 1])

            # schedule[wi][group_t]['cost'] = real_task - real_dis
            return real_task - real_dis

if __name__ == '__main__':

    dw = 3
    dt = 2
    K = 0.05
    worker_speed = 60

    # # Beijing tdrive data
    # file1 = 'task_tdrive.json'
    # file2 = 'LocationTrajectory_tdrive.json'
    # file3 = 'worker_tdrive.json'

    # data_file = 'data-source//'

    # # Berlin data
    # file1 = data_file + 'task_berlin.json'
    # file2 = data_file + 'location-berlin.json'
    # file3 = data_file + 'worker_berlin.json'
    # TaskPoint, LocationTrajectoryPoint, WorkerTrajectory = data_read(file1, file2, file3)

    out_prefix = 'data_result/'
    synthetic_prefix = 'data_result/synthetic_data/'

    # Berlin
    out_stoplocation_file = out_prefix + 'stoplocation_berlin.json'
    out_worker_file = out_prefix + 'worker_berlin.json'
    out_task_file = out_prefix + 'task_berlin.json'

    # Berlin Synthetic
    # out_stoplocation_file = synthetic_prefix + 'RoadVerticesBER.json'   
    # out_worker_file = synthetic_prefix + 'worker_berlin_synthetic.json'    
    # out_task_file = synthetic_prefix + 'task_berlin_synthetic.json'


    # 读取任务的数据
    with open(out_task_file, 'r') as f_task:
        task_dict_1 = json.load(f_task)
    TaskPoint = {}
    for k in task_dict_1.keys():
        if int(k) == 6001:
            break
        TaskPoint[int(k)] = task_dict_1[k]
    print('task dict:', len(TaskPoint))

    # worker 数据
    with open(out_stoplocation_file, 'r') as f_stop:
        stop_dict = json.load(f_stop)
    LocationTrajectoryPoint = defaultdict(list)
    for k in stop_dict.keys():
        LocationTrajectoryPoint[int(k)] = stop_dict[k]

    with open(out_worker_file, 'r') as f_route:
        route_dict = json.load(f_route)
    WorkerTrajectory = defaultdict(list)
    for k in route_dict.keys():
        WorkerTrajectory[int(k)] = route_dict[k]
    print('worker dict:', len(WorkerTrajectory))

    # experiment parameter
    detour_rate = 0.7
    group_size = 5
    group_range = 0.4
    print('size:', group_size, 'range:', group_range, 'rate:', detour_rate)

    start = time.time()
    maps = []
    max_row = []
    min_row = []
    for row in TaskPoint:
        row_i = []
        for low in TaskPoint:
            dis = get_distance_hav(TaskPoint[row]['location'], TaskPoint[low]['location'])
            row_i.append(dis)
        max_row.append(max(row_i))
        min_row.append(min(row_i))
        maps.append(row_i)

    graph = Graph(maps)

    # print('----- greedy group -----')
    greedy_group = graph.baseline_group(group_size, group_range)
    # print('group time:', time.time() - start, 'group length:', len(greedy_group))

    print('======== initial arr ===========')
    start = time.time()
    value = []
    for wj in WorkerTrajectory.keys():
        p = []
        tao = detour_rate * WorkerTrajectory[wj]['distance']
        for j in range(0, len(greedy_group)):
            flag = True

            for t in greedy_group[j]:

                if dist_task_traj(t, WorkerTrajectory[wj]['Trajectory'])[-1] > 0.5 * tao:
                    flag = False
                    break

                min_j, min_dis = dist_task_traj(t, WorkerTrajectory[wj]['Trajectory'])
                min_j = WorkerTrajectory[wj]['Trajectory'].index(min_j)
                dist_seg = 0
                for k in range(0, min_j - 1):
                    l1 = WorkerTrajectory[wj]['Trajectory'][k]
                    l2 = WorkerTrajectory[wj]['Trajectory'][k + 1]
                    dist_seg += get_distance_hav(LocationTrajectoryPoint[l1], LocationTrajectoryPoint[l2])
                dist_seg += get_distance_hav(LocationTrajectoryPoint[WorkerTrajectory[wj]['Trajectory'][min_j]],
                                             TaskPoint[t + 1]['location'])
                if dist_seg / worker_speed > TaskPoint[t + 1]['expiration']:
                    flag = False

            if flag == False:
                value.append((wj, j, 0))
            else:
                value.append((wj, j, len(greedy_group[j])))

    start_time = time.time()
    max_weight, initial_assignment = run_kuhn_munkres(value)
    print('once:', time.time() - start_time)

    print('time:', time.time() - start)
    print('initial assignment:', max_weight)

    be_max_weight = 0
    af_max_weight = max_weight

    be_cost = 0
    af_cost = 1

    schedule = {}
    for k in WorkerTrajectory.keys():
        schedule[k] = {}

    flag_change = True
    flag_count = -1

    time0 = 0
    time00 = 0

    start0000 = time.time()

    while flag_count != 0:

        start_s = time.time()

        be_max_weight = af_max_weight
        be_cost = af_cost
        flag_count = 0

        start0 = time.time()
        for pair in initial_assignment:
            single_schedule = []
            single_detour = []

            if pair[-1] != 0:

                flag = False
                flag_pair = 0
                for wj in schedule.keys():
                    if wj == pair[0]:
                        for group_t in schedule[wj].keys():
                            if group_t == pair[1]:
                                flag = True
                                break

                if flag == True:
                    single_schedule = schedule[pair[0]][pair[1]]['schedule']
                    single_detour = schedule[pair[0]][pair[1]]['detour']

                else:
                    single_schedule, _, single_detour = schedule_pair(pair[0], greedy_group[pair[1]])

                if len(single_schedule) == 0:
                    raw = (pair[0] - 1) * len(greedy_group)
                    line = pair[1]
                    value[raw + line] = value[raw + line][:1] + (value[raw + line][1], 0.0)
                    flag_count += 1

                else:
                    schedule[pair[0]][pair[1]] = {}
                    schedule[pair[0]][pair[1]]['schedule'] = single_schedule
                    schedule[pair[0]][pair[1]]['detour'] = single_detour

        af_max_weight, initial_assignment = run_kuhn_munkres(value)

    total_cost = real_distance(schedule, initial_assignment)
    print('total cost initial:', time.time() - start, af_max_weight, total_cost)

    min_cost = total_cost
    optimal_assignment = []

    value_copy = {}
    for k in value:
        value_copy[k[0]] = {}
    for k in value:
        if k[-1] != 0:
            value_copy[k[0]][k[1]] = real_distance_group(schedule, k[0], k[1])

    total = af_max_weight
    total_new = total

    while af_max_weight == total_new:

        time_round = time.time()

        total, assignment = run_kuhn_munkres(value)
        # print(total, assignment)
        total_temp = total

        max_cost = 0
        max_wi = -1
        max_gt = -1
        for m in assignment:
            if m[-1] != 0 and value_copy[m[0]][m[1]] > max_cost:
                max_cost = value_copy[m[0]][m[1]]
                max_wi = m[0]
                max_gt = m[1]

        for val_copy in value:
            val = val_copy
            if val[-1] != 0 and value_copy[val[0]][val[1]] >= max_cost:
                # count_pair += 1
                value.remove(val)
                val = list(val)
                val.remove(val[-1])
                val.append(0)
                val = tuple(val)
                value.append(val)
        total, assignment = run_kuhn_munkres(value)

        total_cost = real_distance(schedule, assignment)
        print(total, total_cost, len(assignment), max_cost, time.time()-time_round)
        total_new = total

        if total == total_temp and total_cost < min_cost:
            optimal_assignment = assignment
            min_cost = total_cost

    print('Time:', time.time() - start, total_temp, min_cost)

