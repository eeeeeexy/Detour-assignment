from math import sin, asin, cos, radians, fabs, sqrt
import copy
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


def worker_seg_traj_distance(w, s, d, WorkerTrajectory, LocationTrajectoryPoint):
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


def dist_group_traj(group_list, traj_list, TaskPoint, LocationTrajectoryPoint):
    dist_list = []
    for i in group_list:
        loc_t = TaskPoint[i + 1]['location']
        min_dist = 999
        for j in traj_list:
            loc_traj = LocationTrajectoryPoint[j]
            if get_distance_hav(loc_t, loc_traj) < min_dist:
                min_dist = get_distance_hav(loc_t, loc_traj)
        dist_list.append(min_dist)
    return min(dist_list)


def dist_task_traj(task, traj_list, TaskPoint, LocationTrajectoryPoint):
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


def group_travel_cost(group_list, TaskPoint):
    if len(group_list) > 1:
        dist_sum = 0
        for i in range(0, len(group_list) - 1):
            dist_sum += get_distance_hav(TaskPoint[group_list[i] + 1]['location'], TaskPoint[group_list[i + 1] + 1]['location'])
        return dist_sum
    else:
        return 0


def detour_pair(worker_trajectory):
    detour_combin = []
    for i in range(0, len(worker_trajectory) - 1):
        for j in range(i + 1, len(worker_trajectory)):
            detour_combin.append([worker_trajectory[i], worker_trajectory[j]])

    return detour_combin


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


class DFS_hungary():

    def __init__(self, nx, ny, edge, cx, cy, visited):
        self.nx, self.ny = nx, ny
        self.edge = edge
        self.cx, self.cy = cx, cy
        self.visited = visited

    def max_match(self, M):
        # print(M)
        res = 0
        for i in self.nx:
            if self.cx[i] == -1:
                for key in self.ny:  # 将visited置0表示未访问过
                    self.visited[key] = 0
                res += self.path(i, M)
        return res, M

    def path(self, u, M):
        for v in self.ny:
            if self.edge[u][v] and (not self.visited[v]):
                self.visited[v] = 1
                if self.cy[v] == -1:
                    self.cx[u] = v
                    self.cy[v] = u
                    M.append((u, v, self.edge[u][v]))
                    return self.edge[u][v]
                else:
                    if (self.cy[v], v, self.edge[self.cy[v]][v]) in M:
                        M.remove((self.cy[v], v, self.edge[self.cy[v]][v]))
                    if self.path(self.cy[v], M):
                        self.cx[u] = v
                        self.cy[v] = u
                        M.append((u, v, self.edge[u][v]))
                        return self.edge[u][v]
        return 0


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


def ver(tree):
    if len(tree) > 1:
        sum_1 = 0
        sum_2 = 0
        for i in tree.keys():
            for j in tree[i].keys():
                sum_1 += tree[i][j] ** 2
                sum_2 += tree[i][j]
        return sum_1 / (len(tree.keys()) - 1) - (sum_2 / (len(tree.keys()) - 1)) ** 2
    else:
        return -1


def subtree_add(tree, subtree, active_node):
    if len(tree[active_node]) != 0:
        list_nodes = tree[active_node].keys()
        subtree[active_node] = tree[active_node]
        for i in list_nodes:
            if i not in subtree.keys():
                subtree_add(tree, subtree, i)
    else:
        subtree[active_node] = {}

    return subtree


def total_edge(tree):
    count = 0
    for i in tree.keys():
        if len(tree[i].keys()) != 0:
            count += len(tree[i].keys())
    return count


def total_node(tree):
    return len(set(tree.keys()))


def spanning_group(Group, tree, k, alpha):
    
    # 方差
    ver_group = ver(tree)
    if ver_group < 0 or ver_group <= alpha:

        if total_node(tree) <= k:
            Group.append(list(tree.keys()))
        else:
            spanning_group_k(Group, tree, k)

    else:
        sub_max = -1
        opt_edge = []
        for i in tree.keys():
            for j in tree[i].keys():
                tree_copy = copy.deepcopy(tree)
                tree_copy[i].pop(j)
                tree_copy[j].pop(i)
                ver_i = ver(tree_copy)
                sub = ver_group - ver_i
                if sub > sub_max:
                    sub_max = sub
                    opt_edge = [i, j]
        # print('opt edge:', opt_edge, type(opt_edge[0]), type(opt_edge[1]))

        # 初始化两颗子树
        subtree_1 = {}
        subtree_2 = {}

        # 以opt edge的两端的点作为激活点进行子树划分
        # print('initial:', total_edge(tree))
        delete_tree = copy.deepcopy(tree)
        delete_tree[opt_edge[0]].pop(opt_edge[1])
        delete_tree[opt_edge[1]].pop(opt_edge[0])
        # print('length of tree:', total_edge(delete_tree), delete_tree)
        subtree_add(delete_tree, subtree_1, opt_edge[0])
        subtree_add(delete_tree, subtree_2, opt_edge[1])
        # print('subtree 1:', total_edge(subtree_1), total_node(subtree_1), subtree_1)
        # print('subtree 2:', total_edge(subtree_2), total_node(subtree_2), subtree_2)
        group = {}
        group[0] = subtree_1
        group[1] = subtree_2

        # 对子树递归
        for i in group.keys():
            spanning_group(Group, group[i], k, alpha)

    return Group


def spanning_group_k(Group, tree, k):

    # 方差
    ver_group = ver(tree)

    if total_node(tree) <= k:
        Group.append(list(tree.keys()))

    else:
        sub_max = -1
        opt_edge = []
        for i in tree.keys():
            for j in tree[i].keys():
                tree_copy = copy.deepcopy(tree)
                tree_copy[i].pop(j)
                tree_copy[j].pop(i)
                ver_i = ver(tree_copy)
                sub = ver_group - ver_i
                if sub > sub_max:
                    sub_max = sub
                    opt_edge = [i, j]
        # 初始化两颗子树
        subtree_1 = {}
        subtree_2 = {}
        # 以opt edge的两端的点作为激活点进行子树划分
        delete_tree = copy.deepcopy(tree)
        delete_tree[opt_edge[0]].pop(opt_edge[1])
        delete_tree[opt_edge[1]].pop(opt_edge[0])
        subtree_add(delete_tree, subtree_1, opt_edge[0])
        subtree_add(delete_tree, subtree_2, opt_edge[1])
        group = {}
        group[0] = subtree_1
        group[1] = subtree_2
        # 对子树递归
        for i in group.keys():
            spanning_group_k(Group, group[i], k)
    return Group


def real_distance(schedule, assignment, TaskPoint, WorkerTrajectory, LocationTrajectoryPoint):

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
            real_dis = get_distance_hav(LocationTrajectoryPoint[index_d], LocationTrajectoryPoint[index_c])
            # for p in range(index_d, index_c):
            #     real_dis += get_distance_hav(LocationTrajectoryPoint[p], LocationTrajectoryPoint[p + 1])

            # if real_task - real_dis > 0:
            #     total_cost += real_task - real_dis
            total_cost += real_task - real_dis

    return total_cost


def real_distance_group(schedule, wi, group_t, TaskPoint, WorkerTrajectory, LocationTrajectoryPoint):

    l_d = schedule[wi][group_t]['detour'][0]
    l_c = schedule[wi][group_t]['detour'][1]
    index_d = WorkerTrajectory[wi]['Trajectory'].index(l_d)
    index_c = WorkerTrajectory[wi]['Trajectory'].index(l_c)
    schedule_list = schedule[wi][group_t]['schedule']
    # 实际距离
    real_task = 0
    if len(schedule_list) > 1:
        for s in range(0, len(schedule_list) - 1):
            real_task += get_distance_hav(TaskPoint[schedule_list[s] + 1]['location'],
                                          TaskPoint[schedule_list[s + 1] + 1]['location'])
    real_task += get_distance_hav(LocationTrajectoryPoint[l_d], TaskPoint[schedule_list[0] + 1]['location'
    ]) + get_distance_hav(LocationTrajectoryPoint[l_c], TaskPoint[schedule_list[-1] + 1]['location'])
    # 轨迹总距离，实际距离
    real_dis = 0
    for p in range(index_d, index_c):
        real_dis += get_distance_hav(LocationTrajectoryPoint[p], LocationTrajectoryPoint[p + 1])
    return real_task - real_dis


def MST(group_list, trajectory_list, TaskPoint, LocationTrajectoryPoint):
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


def dist_task_schedule(task_schedule_circle, end_task, TaskPoint):
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


def task_deadline_check(group_list, j, best_pair, worker_speed, TaskPoint, WorkerTrajectory, LocationTrajectoryPoint):
    # group_list : []
    # wc : j
    # best_pair : [start, end]

    flag = True

    prefix_dist = worker_seg_traj_distance(j, 0, WorkerTrajectory[j]['Trajectory'].index(best_pair[0]), WorkerTrajectory, LocationTrajectoryPoint)

    # print('worker id:', j, 'task list:', group_list, TaskPoint[group_list[0] + 1]['expiration'])
    # print('prefix:', (prefix_dist + get_distance_hav(LocationTrajectoryPoint[best_pair[0]], TaskPoint[group_list[0] + 1]['location'])) / worker_speed)

    if (prefix_dist + get_distance_hav(LocationTrajectoryPoint[best_pair[0]], TaskPoint[group_list[0] + 1]['location'])) / worker_speed < TaskPoint[group_list[0] + 1]['expiration']:
        flag = False
        # print('worker id:', j)
    
    dist_task = 0
    for task in range(len(group_list)-1):
        start_task = group_list[task]
        next_task = group_list[task + 1]
        dist_task += get_distance_hav(TaskPoint[start_task + 1]['location'], TaskPoint[next_task + 1]['location'])
        if (prefix_dist + dist_task) / worker_speed < TaskPoint[next_task + 1]['expiration']:
            flag = False
            break
    
    return flag