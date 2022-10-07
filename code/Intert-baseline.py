from collections import defaultdict
import json, time, copy
from math import sin, asin, cos, radians, fabs, sqrt
from itertools import combinations
import numpy as np

EARTH_RADIUS = 6371  # 地球平均半径，6371km

def hav(theta):
    s = sin(theta / 2)
    return s * s

def get_distance_hav(location_a, location_b):
    "用haversine公式计算球面两点间的距离。"
    # 经纬度转换成弧度
    lat0 = radians(location_a[0])
    lat1 = radians(location_b[0])
    lng0 = radians(location_a[1])
    lng1 = radians(location_b[1])

    dlng = fabs(lng0 - lng1)
    dlat = fabs(lat0 - lat1)
    h = hav(dlat) + cos(lat0) * cos(lat1) * hav(dlng)
    distance = 2 * EARTH_RADIUS * asin(sqrt(h))

    return distance

class data_process(object):

    def task_data(self, input_task_file):
        with open(input_task_file, 'r', encoding='utf-8') as f1:
            TaskPoint = {}
            counttask = 0
            for line in f1.readlines():
                counttask += 1  # 重新编码POI的序号
                line = line.strip('\n')  # 去掉换行符\n
                b = line.split(' ')  # 将每一行以空格为分隔符转换成列表
                b = list(map(float, b))
                k = counttask
                del b[0]  # 删除POI的序号
                del b[2]
                del b[2]
                # b[2] = int(b[2])
                TaskPoint[k] = {}
                TaskPoint[k]['location'] = b
                TaskPoint[k]['expiration'] = 0
        print('Location of task point:', len(TaskPoint))
        return TaskPoint

    def location_traj_data(self, input_busstop_file):
        with open(input_busstop_file, 'r', encoding='utf-8') as f:
            LocationTrajectoryPoint = {}
            # for line in f.readlines():
            for line in f.readlines():
                line = line.strip('\n')  # 去掉换行符\n
                b = line.split(' ')  # 将每一行以空格为分隔符转换成列表
                # print('bbb:', b, b[0])
                k = int(b[0])
                # print('k:', k)
                del b[0]
                # del b[2]  # real data
                # del b[2]  # real data
                b = list(map(float, b))
                LocationTrajectoryPoint[k] = b
        print('Location of trajectory point:', len(LocationTrajectoryPoint))
        return LocationTrajectoryPoint

    def worker_data(self, input_busroute_file):
        with open(input_busroute_file, 'r', encoding='utf-8') as f:
            WorkerTrajectory = {}
            count_worker = 0
            count = 0
            for line in f.readlines():
                count += 1
                count_worker += 1
                if count_worker == 51:
                    break
                line = line.strip('\n')  # 去掉换行符\n
                b = line.split(' ')  # 将每一行以空格为分隔符转换成列表
                b = list(map(int, b))
                k = count
                del b[0]
                WorkerTrajectory[k] = {}
                WorkerTrajectory[k]['Trajectory'] = b
                dis = 0
                for j in range(0, len(WorkerTrajectory[count]['Trajectory']) - 1):
                    l1 = WorkerTrajectory[count]['Trajectory'][j]
                    l2 = WorkerTrajectory[count]['Trajectory'][j + 1]
                    dis += get_distance_hav(LocationTrajectoryPoint[l1], LocationTrajectoryPoint[l2])
                WorkerTrajectory[k]['distance'] = dis
                # WorkerTrajectory[k]['deadline'] = dw * dis / worker_speed
        print('Worker:', len(WorkerTrajectory))
        return WorkerTrajectory

class Graph(object):
    def __init__(self, maps):
        self.maps = maps  # 初始化邻接矩阵
        self.nodenum = self.get_nodenum()  # 初始化节点数
        self.edgenum = self.get_edgenum()  # 初始化边数
        self.node = self.node

    def node(self):
        return {"id:": 0, "loc:": [0, 0], "tag:": 0}

    # 获取节点数
    def get_nodenum(self):
        return len(self.maps)

    # 获取边数
    def get_edgenum(self):
        count = 0  # 初始化边数
        for i in range(self.nodenum):
            for j in range(i):
                if self.maps[i][j] > 0 and self.maps[i][j] < 9999:  # 生成边的条件是0<边的权重<9999
                    # print(i, j, maps[i][j])	#打印行列及权重
                    count += 1  # 边数+1
        return count

    # prim算法
    def prim(self):
        res = []  # 初始化最小生成树
        if self.nodenum <= 0 or self.edgenum < self.nodenum - 1:  # 若节点数<=0或边数<节点数-1，则
            return res
        seleted_node = [0]  # 初始化入围节点0，为起始节点，可任选
        candidate_node = [i for i in range(1, self.nodenum)]  # 生成候选节点1-5(去除已入围节点)
        while len(candidate_node) > 0:  # 当候选节点不为空时循环
            begin, end, minweight = 0, 0, 9999  # 初始化边的头节点、尾节点、最小权重
            for i in seleted_node:  # 遍历头节点在入围节点、尾节点在候选节点的边
                for j in candidate_node:
                    if self.maps[i][j] < minweight:  # 若当前边的权重<最小权重，则
                        minweight = self.maps[i][j]  # 更新最小权重值
                        begin = i  # 更新边的头节点
                        end = j  # 更新边的尾节点
            res.append([begin, end, minweight])  # 将头节点、尾节点、最小权重添加到最小生成树中
            seleted_node.append(end)  # 将当前尾节点添加到入围节点中
            candidate_node.remove(end)  # 从候选节点中移除当前尾节点，然后继续循环

        # 用字典存储数据结构
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

    # 分组基线算法
    def baseline_group(self, k, size):
        Group = []
        seleted_node = [i for i in range(0, self.nodenum)]
        candidate_node = [i for i in range(0, self.nodenum)]  # 存储未被标记的节点
        while len(candidate_node) > 0:  # 当候选节点不为空时循环
            for i in seleted_node:  # 遍历头节点在入围节点、尾节点在候选节点的边
                if i in candidate_node: # 如果未被标记
                    group_i = [i]
                    p = k
                    while p > 1:
                        minweight = 999
                        min_node = -1
                        for j in candidate_node:
                            if i != j and self.maps[i][j] < size and self.maps[i][j] < minweight:
                                minweight = self.maps[i][j]  # 更新最小权重值
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

        #选取较小的作为x
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
    # print('matrix:', len(x_y_values))
    process = KuhnMunkres()
    process.set_matrix(x_y_values)
    # print('matrix later:', len(x_y_values))
    process.km()
    return process.get_max_value_result(), process.get_connect_result()

def real_distance(schedule, assignment):

    total_cost = 0

    for i in range(0, len(assignment)):

        if assignment[i][2] != 0:

            # 总的绕路距离，欧式距离
            # total_cost_ed += schedule[assignment[i][0]][assignment[i][1]]['cost']

            l_d = schedule[assignment[i][0]][assignment[i][1]]['detour'][0]
            l_c = schedule[assignment[i][0]][assignment[i][1]]['detour'][1]
            index_d = WorkerTrajectory[assignment[i][0]]['Trajectory'].index(l_d)
            index_c = WorkerTrajectory[assignment[i][0]]['Trajectory'].index(l_c)

            schedule_list = schedule[assignment[i][0]][assignment[i][1]]['schedule']

            # detour 总距离，实际距离
            real_task = 0
            real_task_ed = 0
            if len(schedule_list) > 1:
                for s in range(0, len(schedule_list) - 1):
                    real_task += get_distance_hav(TaskPoint[schedule_list[s]+1]['location'], TaskPoint[schedule_list[s + 1]+1]['location'])

            real_task += get_distance_hav(LocationTrajectoryPoint[l_d], TaskPoint[schedule_list[0]+1]['location'
            ]) + get_distance_hav(LocationTrajectoryPoint[l_c], TaskPoint[schedule_list[-1]+1]['location'])

            # 轨迹总距离，实际距离
            real_dis = 0
            for p in range(index_d, index_c):
                real_dis += get_distance_hav(LocationTrajectoryPoint[p], LocationTrajectoryPoint[p + 1])

            # if real_task - real_dis > 0:
            #     total_cost += real_task - real_dis
            total_cost += real_task - real_dis

    return total_cost


def MST(group_list, trajectory_list):
    # print('group list:', group_list)
    # print('trajectory list:', trajectory_list)
    # 将 任务位置数据 和 轨迹数据 整理为全连接邻接矩阵
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

# DFS算法
def DFS(spanning_tree, flag, s):  # 图 s指的是开始结点

    direct_graph = []
    # 需要一个队列
    stack = []
    stack.append(s)
    seen = set()  # 看是否访问过
    seen.add(s)

    while (len(stack) > 0):
        # 拿出邻接点
        vertex = stack.pop()  # 这里pop参数没有0了，最后一个元素
        nodes = spanning_tree[vertex]
        for w in nodes:
            if w not in seen:  # 如何判断是否访问过，使用一个数组
                stack.append(w)
                seen.add(w)

        if flag[vertex] == 0 and vertex not in direct_graph:
            direct_graph.append(vertex)

    return direct_graph

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

def worker_seg_traj_distance(w, s, d):
    dis = 0
    temp = -1
    if d < s:
        temp = s
        s = d
        d = temp
    else:
        for k in range(s, d):
            l1 = WorkerTrajectory[w]['Trajectory'][k]
            l2 = WorkerTrajectory[w]['Trajectory'][k+1]
            dis += get_distance_hav(LocationTrajectoryPoint[l1], LocationTrajectoryPoint[l2])
    return dis


def schedule_pair(j, group_greedy):

    tao = detour_rate * WorkerTrajectory[j]['distance']
    # tao = 0.2

    assignment = {}
    j_assignment = {}

    # Assignment[j] = {}

    group_id = 0

    for group_list in group_greedy:
        group_id += 1

        # 对 group 剪枝
        flag = True
        for i in group_list:
            if dist_task_traj(i, WorkerTrajectory[j]['Trajectory'])[-1] > 0.5 * tao:
                flag = False
                break

            # group 时间剪枝策略
            min_j, min_dis = dist_task_traj(i, WorkerTrajectory[j]['Trajectory'])
            min_j = WorkerTrajectory[j]['Trajectory'].index(min_j)
            dist_seg = 0
            for k in range(0, min_j - 1):
                l1 = WorkerTrajectory[j]['Trajectory'][k]
                l2 = WorkerTrajectory[j]['Trajectory'][k + 1]
                dist_seg += get_distance_hav(LocationTrajectoryPoint[l1], LocationTrajectoryPoint[l2])
            dist_seg += get_distance_hav(LocationTrajectoryPoint[WorkerTrajectory[j]['Trajectory'][min_j]],
                                            TaskPoint[i + 1]['location'])
            if dist_seg / worker_speed > TaskPoint[i + 1]['expiration']:
                flag = False

        if flag == False: 
            continue

        # 当 group 合格
        else:

            # 当 group的规模超过一个 task
            if len(group_list) > 1:

                spanning_tree, flag_tree = MST(group_list, WorkerTrajectory[j]['Trajectory'])
                task_schedule_circle = DFS(spanning_tree, flag_tree, group_list[0])

                optimal_cost = 99  # 减去中间一段的 cost
                optimal_start_task = -1
                optimal_detour = []

                # group 离轨迹的最近距离
                near_dist = 99
                # n_e = -1
                for t in task_schedule_circle:
                    for o in range(0, len(WorkerTrajectory[j]['Trajectory'])):
                        dist_n = get_distance_hav(LocationTrajectoryPoint[WorkerTrajectory[j]['Trajectory'][o]],
                                                TaskPoint[t + 1]['location'])
                        if dist_n < near_dist:
                            near_dist = dist_n
                            # n_e = o

                for s in range(1, len(task_schedule_circle)):

                    start_task = task_schedule_circle[s]
                    end_task = task_schedule_circle[s - 1]

                    # group 中的环形的任务的总距离
                    dist_2 = dist_task_schedule(task_schedule_circle, end_task)

                    candidate_traj = []
                    for o in range(0, len(WorkerTrajectory[j]['Trajectory'])):

                        # 应用 tao - near_dist - dist_2作为一个 bound，因为near_dist是一个 lower bound
                        if get_distance_hav(LocationTrajectoryPoint[WorkerTrajectory[j]['Trajectory'][o]],
                            TaskPoint[start_task + 1]['location']) <= tao - near_dist - dist_2 or get_distance_hav(
                            LocationTrajectoryPoint[WorkerTrajectory[j]['Trajectory'][o]],
                            TaskPoint[end_task + 1]['location']) <= tao - near_dist - dist_2:

                            # 存储符合限制要求的轨迹点属于作为候选轨迹点
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
                            if dist_5 < tao:
                                # print('pair:', l, r, dist_5, dist_5 - worker_seg_traj_distance(j, l, r))
                                best_detour_pair = [WorkerTrajectory[j]['Trajectory'][candidate_traj[l]],
                                                    WorkerTrajectory[j]['Trajectory'][candidate_traj[r]]]
                                min_cost = dist_5 - worker_seg_traj_distance(j, candidate_traj[l], candidate_traj[r])
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

                    # Assignment[j][group_id] = {}
                    # Assignment[j][group_id]['schedule'] = right_list
                    # Assignment[j][group_id]['detour'] = optimal_detour
                    j_assignment[group_id] = {}
                    j_assignment[group_id]['schedule'] = right_list
                    j_assignment[group_id]['detour'] = optimal_detour


            # 当 group中只有一个任务
            if len(group_list) == 1:
                task_center = group_list[0]

                # 遍历轨迹点从左到右
                near_dist = 99
                # group 离轨迹的最近距离
                # n_e = -1
                for oo in range(0, len(WorkerTrajectory[j]['Trajectory'])):

                    dist_4 = get_distance_hav(LocationTrajectoryPoint[WorkerTrajectory[j]['Trajectory'][oo]],
                                            TaskPoint[task_center + 1]['location'])
                    if dist_4 < near_dist:
                        near_dist = dist_4
                        # n_e = oo

                # 根据绕路限制，筛选可达的轨迹点
                candidate_traj = []
                for o in range(0, len(WorkerTrajectory[j]['Trajectory'])):
                    if get_distance_hav(LocationTrajectoryPoint[WorkerTrajectory[j]['Trajectory'][o]],
                                        TaskPoint[task_center + 1]['location']) < tao - near_dist:
                        candidate_traj.append(o)

                # 从左和右同时相对遍历，一旦有轨迹对符合绕路限制，停止遍历，返回轨迹对
                best_detour_pair = []
                # optimal_cost = -1
                for l in range(0, len(candidate_traj)):
                    for r in range(len(candidate_traj) - 1, l, -1):
                        dist_5 = get_distance_hav(
                            LocationTrajectoryPoint[WorkerTrajectory[j]['Trajectory'][candidate_traj[l]]],
                            TaskPoint[task_center + 1]['location']) + get_distance_hav(
                            LocationTrajectoryPoint[WorkerTrajectory[j]['Trajectory'][candidate_traj[r]]],
                            TaskPoint[task_center + 1]['location'])
                        if dist_5 < tao:
                            best_detour_pair = [WorkerTrajectory[j]['Trajectory'][candidate_traj[l]],
                                                WorkerTrajectory[j]['Trajectory'][candidate_traj[r]]]
                            # optimal_cost = dist_5 - worker_seg_traj_distance(j, candidate_traj[l], candidate_traj[r])
                            break
                    break

                if len(candidate_traj) > 1:
                    # Assignment[j][group_id] = {}
                    # Assignment[j][group_id]['schedule'] = [task_center]
                    # Assignment[j][group_id]['detour'] = best_detour_pair
                    j_assignment[group_id] = {}
                    j_assignment[group_id]['schedule'] = [task_center]
                    j_assignment[group_id]['detour'] = best_detour_pair
    assignment[j] = j_assignment
    return assignment


def pair_route(j, group_order):

    tao = detour_rate * WorkerTrajectory[j]['distance']
    # tao = 0.2

    # 一个group的所有组合
    group_com = [group_order[:v] for v in range(1, len(group_order) + 1)]

    assignment = {}
    j_assignment = {}

    # Assignment[j] = {}

    group_id = 0

    for group_list in group_com:
        group_id += 1

        # 对 group 剪枝
        flag = True
        for i in group_list:
            if dist_task_traj(i, WorkerTrajectory[j]['Trajectory'])[-1] > 0.5 * tao:
                flag = False
                break

            # group 时间剪枝策略
            min_j, min_dis = dist_task_traj(i, WorkerTrajectory[j]['Trajectory'])
            min_j = WorkerTrajectory[j]['Trajectory'].index(min_j)
            dist_seg = 0
            for k in range(0, min_j - 1):
                l1 = WorkerTrajectory[j]['Trajectory'][k]
                l2 = WorkerTrajectory[j]['Trajectory'][k + 1]
                dist_seg += get_distance_hav(LocationTrajectoryPoint[l1], LocationTrajectoryPoint[l2])
            dist_seg += get_distance_hav(LocationTrajectoryPoint[WorkerTrajectory[j]['Trajectory'][min_j]],
                                            TaskPoint[i + 1]['location'])
            if dist_seg / worker_speed > TaskPoint[i + 1]['expiration']:
                flag = False

        if flag == False: 
            continue

        # 当 group 合格
        else:
            
            # 当 group的规模超过一个 task
            if len(group_list) > 1:

                spanning_tree, flag_tree = MST(group_list, WorkerTrajectory[j]['Trajectory'])
                task_schedule_circle = DFS(spanning_tree, flag_tree, group_list[0])

                optimal_cost = 99  # 减去中间一段的 cost
                optimal_start_task = -1
                optimal_detour = []

                # group 离轨迹的最近距离
                near_dist = 99
                # n_e = -1
                for t in task_schedule_circle:
                    for o in range(0, len(WorkerTrajectory[j]['Trajectory'])):
                        dist_n = get_distance_hav(LocationTrajectoryPoint[WorkerTrajectory[j]['Trajectory'][o]],
                                                TaskPoint[t + 1]['location'])
                        if dist_n < near_dist:
                            near_dist = dist_n
                            # n_e = o

                for s in range(1, len(task_schedule_circle)):

                    start_task = task_schedule_circle[s]
                    end_task = task_schedule_circle[s - 1]

                    # group 中的环形的任务的总距离
                    dist_2 = dist_task_schedule(task_schedule_circle, end_task)

                    candidate_traj = []
                    for o in range(0, len(WorkerTrajectory[j]['Trajectory'])):

                        # 应用 tao - near_dist - dist_2作为一个 bound，因为near_dist是一个 lower bound
                        if get_distance_hav(LocationTrajectoryPoint[WorkerTrajectory[j]['Trajectory'][o]],
                            TaskPoint[start_task + 1]['location']) <= tao - near_dist - dist_2 or get_distance_hav(
                            LocationTrajectoryPoint[WorkerTrajectory[j]['Trajectory'][o]],
                            TaskPoint[end_task + 1]['location']) <= tao - near_dist - dist_2:

                            # 存储符合限制要求的轨迹点属于作为候选轨迹点
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
                            if dist_5 < tao:
                                # print('pair:', l, r, dist_5, dist_5 - worker_seg_traj_distance(j, l, r))
                                best_detour_pair = [WorkerTrajectory[j]['Trajectory'][candidate_traj[l]],
                                                    WorkerTrajectory[j]['Trajectory'][candidate_traj[r]]]
                                min_cost = dist_5 - worker_seg_traj_distance(j, candidate_traj[l], candidate_traj[r])
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

                    # Assignment[j][group_id] = {}
                    # Assignment[j][group_id]['schedule'] = right_list
                    # Assignment[j][group_id]['detour'] = optimal_detour
                    j_assignment[group_id] = {}
                    j_assignment[group_id]['schedule'] = right_list
                    j_assignment[group_id]['detour'] = optimal_detour


            # 当 group中只有一个任务
            if len(group_list) == 1:
                task_center = group_list[0]

                # 遍历轨迹点从左到右
                near_dist = 99
                # group 离轨迹的最近距离
                # n_e = -1
                for oo in range(0, len(WorkerTrajectory[j]['Trajectory'])):

                    dist_4 = get_distance_hav(LocationTrajectoryPoint[WorkerTrajectory[j]['Trajectory'][oo]],
                                            TaskPoint[task_center + 1]['location'])
                    if dist_4 < near_dist:
                        near_dist = dist_4
                        # n_e = oo

                # 根据绕路限制，筛选可达的轨迹点
                candidate_traj = []
                for o in range(0, len(WorkerTrajectory[j]['Trajectory'])):
                    if get_distance_hav(LocationTrajectoryPoint[WorkerTrajectory[j]['Trajectory'][o]],
                                        TaskPoint[task_center + 1]['location']) < tao - near_dist:
                        candidate_traj.append(o)

                # 从左和右同时相对遍历，一旦有轨迹对符合绕路限制，停止遍历，返回轨迹对
                best_detour_pair = []
                # optimal_cost = -1
                for l in range(0, len(candidate_traj)):
                    for r in range(len(candidate_traj) - 1, l, -1):
                        dist_5 = get_distance_hav(
                            LocationTrajectoryPoint[WorkerTrajectory[j]['Trajectory'][candidate_traj[l]]],
                            TaskPoint[task_center + 1]['location']) + get_distance_hav(
                            LocationTrajectoryPoint[WorkerTrajectory[j]['Trajectory'][candidate_traj[r]]],
                            TaskPoint[task_center + 1]['location'])
                        if dist_5 < tao:
                            best_detour_pair = [WorkerTrajectory[j]['Trajectory'][candidate_traj[l]],
                                                WorkerTrajectory[j]['Trajectory'][candidate_traj[r]]]
                            # optimal_cost = dist_5 - worker_seg_traj_distance(j, candidate_traj[l], candidate_traj[r])
                            break
                    break

                if len(candidate_traj) > 1:
                    # Assignment[j][group_id] = {}
                    # Assignment[j][group_id]['schedule'] = [task_center]
                    # Assignment[j][group_id]['detour'] = best_detour_pair
                    j_assignment[group_id] = {}
                    j_assignment[group_id]['schedule'] = [task_center]
                    j_assignment[group_id]['detour'] = best_detour_pair
    assignment[j] = j_assignment
    print(assignment[j])
    return assignment

def detour_pair(worker_trajectory):
    detour_combin = []
    for i in range(0, len(worker_trajectory) - 1):
        for j in range(i + 1, len(worker_trajectory)):
            detour_combin.append([worker_trajectory[i], worker_trajectory[j]])

    return detour_combin


def task_deadline_check(group_list, j, best_pair, worker_speed):
    # group_list : []
    # wc : j
    # best_pair : [start, end]

    flag = True

    prefix_dist = worker_seg_traj_distance(j, 0, WorkerTrajectory[j]['Trajectory'].index(best_pair[0]))

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







def insert_baseline(group_greedy):

    Assignment = defaultdict(dict)

    for j in WorkerTrajectory.keys():
    # for j in range(31, 32):

        # worker 轨迹上的所有轨迹对组合
        traj_pair_combine = detour_pair(WorkerTrajectory[j]['Trajectory'])

        # 第 j 个 worker的绕路总距离
        tao = detour_rate * WorkerTrajectory[j]['distance']

        Assignment[j] = defaultdict(dict)

        group_id = 0
        for group_list in group_greedy:
            group_id += 1

            group_com = [group_list[:v] for v in range(1, len(group_list) + 1)]

            for group in group_com:

                flag = True
                
                # 当 group中只有一个任务
                if len(group) == 1:

                    task_center = group[0]

                    # 遍历轨迹点从左到右, 找到离任务最近的轨迹点
                    near_dist = 99
                    for traj in WorkerTrajectory[j]['Trajectory']:
                        dist_4 = get_distance_hav(LocationTrajectoryPoint[traj], TaskPoint[task_center + 1]['location'])
                        if dist_4 < near_dist:
                            near_dist = dist_4

                    # 根据绕路限制，筛选可达的轨迹点
                    candidate_traj = []
                    for o in range(0, len(WorkerTrajectory[j]['Trajectory'])):
                        if get_distance_hav(LocationTrajectoryPoint[WorkerTrajectory[j]['Trajectory'][o]],
                                        TaskPoint[task_center + 1]['location']) < tao - near_dist:
                            candidate_traj.append(o)

                    # 从左和右同时相对遍历，一旦有轨迹对符合绕路限制，停止遍历，返回轨迹对
                    best_detour_pair = []
                    optimal_detour_cost = 99
                    for l in range(0, len(candidate_traj)):
                        for r in range(len(candidate_traj) - 1, l, -1):
                            dist_5 = get_distance_hav(
                                LocationTrajectoryPoint[WorkerTrajectory[j]['Trajectory'][candidate_traj[l]]],
                                TaskPoint[task_center + 1]['location']) + get_distance_hav(
                                LocationTrajectoryPoint[WorkerTrajectory[j]['Trajectory'][candidate_traj[r]]],
                                TaskPoint[task_center + 1]['location'])
                            if dist_5 < tao and task_deadline_check(group, j, [WorkerTrajectory[j]['Trajectory'][candidate_traj[l]],
                                                    WorkerTrajectory[j]['Trajectory'][candidate_traj[r]]], worker_speed) == True:
                                # print('pair:', l, r, dist_5, dist_5 - worker_seg_traj_distance(j, l, r))
                                best_detour_pair = [WorkerTrajectory[j]['Trajectory'][candidate_traj[l]],
                                                    WorkerTrajectory[j]['Trajectory'][candidate_traj[r]]]
                                break
                        break

                    if len(candidate_traj) > 1 and len(best_detour_pair) > 0:
                        Assignment[j][group_id] = {}
                        Assignment[j][group_id]['schedule'] = [task_center]
                        Assignment[j][group_id]['detour'] = best_detour_pair

                else:
                    # 当
                    # 采用近似算法生成group和轨迹点的最小生成树
                    spanning_tree, flag_tree = MST(group, WorkerTrajectory[j]['Trajectory'])
                    # 应用 DFS得到 group完成的闭环
                    task_schedule_circle = DFS(spanning_tree, flag_tree, group[0])

                    optimal_detour_cost = 99  # 不减轨迹段
                    optimal_start_task = -1
                    optimal_pair = []

                    # 遍历group的组合
                    for s in range(1, len(task_schedule_circle)):

                        start_task = task_schedule_circle[s]
                        end_task = task_schedule_circle[s - 1]

                        # group 内部完成所有任务的总距离
                        dist_circle = dist_task_schedule(task_schedule_circle, end_task)

                        for pair in traj_pair_combine:

                            # 偏移对到 group的两段距离
                            dist_start = get_distance_hav(LocationTrajectoryPoint[pair[0]], TaskPoint[start_task + 1]['location'])
                            dist_end = get_distance_hav(LocationTrajectoryPoint[pair[-1]], TaskPoint[end_task + 1]['location'])

                            if dist_start + dist_circle + dist_end <= tao:
                                available_cost = dist_start + dist_circle + dist_end
                                available_pair = pair

                                traj_dist = worker_seg_traj_distance(j, WorkerTrajectory[j]['Trajectory'].index(available_pair[0]),
                                            WorkerTrajectory[j]['Trajectory'].index(available_pair[-1]))

                                if available_cost - traj_dist < optimal_detour_cost and task_deadline_check(group, j, available_pair, worker_speed) == True:
                                    optimal_detour_cost = available_cost - traj_dist
                                    optimal_start_task = start_task
                                    optimal_pair = available_pair

                    if optimal_start_task != -1 and len(group) == len(group_list):

                        left_list = []
                        right_list = []
                        for i in range(0, len(task_schedule_circle)):
                            if task_schedule_circle.index(task_schedule_circle[i]) >= task_schedule_circle.index(
                                    optimal_start_task):
                                right_list.append(task_schedule_circle[i])
                            else:
                                left_list.append(task_schedule_circle[i])
                        right_list.extend(left_list)

                        Assignment[j][group_id] = {}
                        Assignment[j][group_id]['schedule'] = right_list
                        Assignment[j][group_id]['detour'] = optimal_pair
                        # Assignment[j][group_id]['cost'] = optimal_detour_cost
            # break

    return Assignment



if __name__ == '__main__':

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
    print('worker dict:', len(WorkerTrajectory), len(LocationTrajectoryPoint))


    # experiment parameter
    worker_speed = 60
    group_size = 5
    group_range = 0.4
    detour_rate = 0.7
    spanning_alpha = 0.0001

    start = time.time()
    maps = []
    max_row = []
    min_row = []
    for row in TaskPoint:
        row_i = []
        for low in TaskPoint:
            # dis = Euclidean_fun(TaskPoint[row]['location'], TaskPoint[low]['location'])
            dis = get_distance_hav(TaskPoint[row]['location'], TaskPoint[low]['location'])
            row_i.append(dis)
        max_row.append(max(row_i))
        min_row.append(min(row_i))
        maps.append(row_i)

    graph = Graph(maps)  # 实例化邻接矩阵
    # print('邻接矩阵为\n%s' % graph.maps)
    print('节点数为%d，边数为%d。\n' % (graph.nodenum, graph.edgenum))

    print('----- greedy grouping -----')
    greedy_start = time.time()
    group_result = graph.baseline_group(group_size, group_range)
    print('group time:', time.time() - start, time.time() - greedy_start, len(group_result))
    group_file = 'data_result/task_group.txt'
    with open(group_file, 'w') as syn_file:
        for group in group_result:
            gr_list = []
            for gr in group:
                gr_list.append(str(gr))
            syn_file.write(str('\t'.join(gr_list)) + '\n')

    print('====== grouping result =====')
    greedy_group = group_result
    # greedy_group = Group_spanning
    # greedy_group = DBSCAN_grouping

    print('============ insert baseline ============')
    start_time = time.time()
    schedule_a = insert_baseline(greedy_group)
    print('schedule time:', time.time() - start_time)

    value = []
    for i in schedule_a.keys():
        for j in range(1, len(greedy_group) + 1):
            if j in schedule_a[i].keys():
                value.append((i, j, len(schedule_a[i][j]['schedule'])))
            else:
                value.append((i, j, 0))
    total, assignment = run_kuhn_munkres(value)
    total_cost = real_distance(schedule_a, assignment)
    print('insert baseline time:', time.time() - start_time, 'total:', total, 'total cost:', total_cost)

