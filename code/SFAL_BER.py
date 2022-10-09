# %%
import math, time, copy, json, os, sys
from tempfile import tempdir
import numpy as np

import sklearn.cluster as skc
from collections import defaultdict
import multiprocessing


from utils import get_distance_hav
from utils import worker_seg_traj_distance
from utils import dist_task_traj
from utils import detour_pair
from utils import task_deadline_check

from utils import run_kuhn_munkres
from utils import Graph
from utils import real_distance, real_distance_group

from utils import MST, DFS, dist_task_schedule, spanning_group


from Exact_basline import exact_algorithm

from PNN_baseline import pnn_baseline

from My_methods import approximate_algorithm, approximate_pruning_group, approximate_pruning_group_traj

# %%
worker_speed = 60
# capacity = 5
rounds = 1

# %%
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
            if dist_task_traj(i, WorkerTrajectory[j]['Trajectory'], TaskPoint, LocationTrajectoryPoint)[-1] > 0.5 * tao:
                flag = False
                break


        if flag == False:
            continue

        # 当 group 合格
        else:

            # 当 group的规模超过一个 task
            if len(group_list) > 1:

                spanning_tree, flag_tree = MST(group_list, WorkerTrajectory[j]['Trajectory'], TaskPoint, LocationTrajectoryPoint)
                task_schedule_circle = DFS(spanning_tree, flag_tree, group_list[0])


                optimal_cost = 99  # 减去中间一段的 cost
                optimal_start_task = -1
                optimal_detour = []

                # group 离轨迹的最近距离
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

                    # group 中的环形的任务的总距离
                    dist_2 = dist_task_schedule(task_schedule_circle, end_task, TaskPoint)

                    candidate_traj = []
                    for o in range(0, len(WorkerTrajectory[j]['Trajectory'])):

                        # 应用 tao - near_dist - dist_2作为一个 bound，因为near_dist是一个 lower bound
                        if get_distance_hav(LocationTrajectoryPoint[WorkerTrajectory[j]['Trajectory'][o]],
                            TaskPoint[start_task + 1]['location']) <= tao - near_dist - dist_2 or \
                            get_distance_hav(LocationTrajectoryPoint[WorkerTrajectory[j]['Trajectory'][o]],
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

                    # check deadline

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
                    j_assignment[group_id] = {}
                    j_assignment[group_id]['schedule'] = [task_center]
                    j_assignment[group_id]['detour'] = best_detour_pair
    
    assignment[j] = j_assignment
    return assignment


def approximate_pruning_group_traj_parallel(group_greedy, WorkerTrajectory):

    Assignment = {}

    # parallel
    pool = multiprocessing.Pool(8)

    result = [pool.apply_async(schedule_pair, args=(j, group_greedy, )) for j in WorkerTrajectory.keys()]
    root = [r.get() for r in result]

    pool.close()
    pool.join()

    for dic in root:
        for key in dic:
            Assignment[key] = dic[key]

    return Assignment


def single_pair_cost(wi, gj, schedule, TaskPoint, WorkerTrajectory, LocationTrajectoryPoint):

    # 总的绕路距离，欧式距离
    # total_cost_ed += schedule[assignment[i][0]][assignment[i][1]]['cost']

    l_d = schedule[wi][gj]['detour'][0]
    l_c = schedule[wi][gj]['detour'][1]
    index_d = WorkerTrajectory[wi]['Trajectory'].index(l_d)
    index_c = WorkerTrajectory[wi]['Trajectory'].index(l_c)

    schedule_list = schedule[wi][gj]['schedule']

    # detour 总距离，实际距离
    real_task = 0
    if len(schedule_list) > 1:
        for s in range(0, len(schedule_list) - 1):
            real_task += get_distance_hav(TaskPoint[schedule_list[s]+1]['location'], TaskPoint[schedule_list[s + 1]+1]['location'])

    real_task += get_distance_hav(LocationTrajectoryPoint[l_d], TaskPoint[schedule_list[0]+1]['location'
    ]) + get_distance_hav(LocationTrajectoryPoint[l_c], TaskPoint[schedule_list[-1]+1]['location'])

    # 轨迹总距离，实际距离
    # real_dis = get_distance_hav(LocationTrajectoryPoint[index_d], LocationTrajectoryPoint[index_c])
    real_dis = 0
    for p in range(index_d, index_c):
        real_dis += get_distance_hav(LocationTrajectoryPoint[p], LocationTrajectoryPoint[p + 1])
    
    return real_task - real_dis


def greedy_assignment(Assignment_schedule):

    pairs_list = []
    for wi in Assignment_schedule.keys():
        for gj in Assignment_schedule[wi].keys():
            
            single_cost = single_pair_cost(wi, gj, Assignment_schedule, TaskPoint, WorkerTrajectory, LocationTrajectoryPoint)
            if single_cost > 0:
                tuple = (wi, gj, len(Assignment_schedule[wi][gj]['schedule']), single_cost)
                pairs_list.append(tuple)
            else:
                tuple = (wi, gj, len(Assignment_schedule[wi][gj]['schedule']), abs(single_cost))
                pairs_list.append(tuple)
            
            # print(tuple)

    sorted_paires = sorted(pairs_list, key=lambda t:(t[2], -t[3]), reverse=True)
    # print(sorted_paires)

    greedy_assign_list = []
    worker_list = []
    group_list = []
    total_cost = 0
    assigned_num = 0
    for pair in sorted_paires:
        if pair[0] not in worker_list and pair[1] not in group_list:
            greedy_assign_list.append(pair)
            worker_list.append(pair[0])
            group_list.append(pair[1])
            total_cost += single_pair_cost(pair[0], pair[1], Assignment_schedule, TaskPoint, WorkerTrajectory, LocationTrajectoryPoint)
            assigned_num += pair[2]
    
    print('num', assigned_num, 'total cost:', total_cost, 'average cost:', (total_cost / assigned_num) * 1000, '(m)')

# %%
dw = 3
dt = 4

# experiment parameter
group_size = 5

group_range = 0.4

detour_rate = 0.7

spanning_alpha = 0.0001
print('group size:', group_size, 'group_range:', group_range)

out_prefix = '/home/xieyuan/task-assignment_paper_code/Detour-assignment/data_result/'
synthetic_prefix = '/home/xieyuan/task-assignment_paper_code/Detour-assignment/data_result/synthetic_data/'

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

graph = Graph(maps)  # 实例化邻接矩阵
# print('邻接矩阵为\n%s' % graph.maps)
print('节点数为%d, 边数为%d。\n' % (graph.nodenum, graph.edgenum))

print('----- greedy grouping -----')
greedy_start = time.time()
greedy_group_result = graph.baseline_group(group_size, group_range)
print('group time:', time.time() - start, time.time() - greedy_start, len(greedy_group_result))

# %%
count = 0
for i in range(0, len(greedy_group_result)):
    count += len(greedy_group_result[i])
print('greedy count:', count)

# print('----- spanning grouping -----')
# spanning_tree = graph.prim()
# Group = []
# Group_spanning = spanning_group(Group, spanning_tree, group_size, spanning_alpha)
# print('spanning group:', Group_spanning)

# fp = open('spanning_group_1500.txt','r')
# Group_spanning = []
# for line in fp:
#     line = line.strip('\n')
#     Group_spanning.append(list(map(int, line.split(' ')[0:-1])))

print('======= DBSCAN grouping =========')

dbscan_start_time = time.time()
DBSCAN_grouping = []
task_datas = []
for task in TaskPoint.keys():
    task_datas.append(TaskPoint[task]['location'])
print('task datas:', len(task_datas))

X = np.array(task_datas)

db = skc.DBSCAN(eps=0.004, min_samples=1).fit(X) #DBSCAN聚类方法 还有参数，matric = ""距离计算方法
labels = db.labels_  #和X同一个维度，labels对应索引序号的值 为她所在簇的序号。若簇编号为-1，表示为噪声

# print('每个样本的簇标号:')
# print(labels)

raito = len(labels[labels[:] == -1]) / len(labels)  #计算噪声点个数占总数的比例

n_clusters_ = len(set(labels)) - (1 if -1 in labels else 0)  # 获取分簇的数目

print('分簇的数目: %d' % n_clusters_)

for i in range(n_clusters_):
    one_cluster = X[labels == i]
    label_index = []
    for n in one_cluster:
        label_index.append(np.where(X == n)[0][0])
    DBSCAN_grouping.append(label_index)

print('dbscan time:', time.time() - dbscan_start_time)

print('====== grouping result =====')
greedy_group = greedy_group_result
# greedy_group = Group_spanning
# greedy_group = DBSCAN_grouping

print('group size:', len(greedy_group))

# %%
print('============ approximate ============')

average_time = 0
for round in range(rounds):
    start_time = time.time()
    schedule_app = approximate_algorithm(greedy_group, detour_rate, TaskPoint, WorkerTrajectory, LocationTrajectoryPoint)
    greedy_assignment(schedule_app)
    # print('schedule time:', time.time() - start_time)
    average_time += time.time() - start_time

average_time /= rounds
print('average time:', average_time)


# %%
print('============ approximate + pruning group ============')
average_time = 0
for round in range(rounds):
    start_time = time.time()
    schedule = approximate_pruning_group(greedy_group, detour_rate, TaskPoint, WorkerTrajectory, LocationTrajectoryPoint)
    greedy_assignment(schedule)
    # print('schedule time:', time.time() - start_time)
    average_time += time.time() - start_time

average_time /= rounds
print('average time:', average_time)

# %%
print('============ approximate + pruning group + pruning traj ============')

average_time = 0
for round in range(rounds):
    start_time = time.time()
    Assignment_approximate_pruning = approximate_pruning_group_traj(greedy_group, detour_rate, TaskPoint, WorkerTrajectory, LocationTrajectoryPoint)
    greedy_assignment(Assignment_approximate_pruning)
    # print('schedule time:', time.time() - start_time)
    average_time += time.time() - start_time

average_time /= rounds
print('average time:', average_time)

# %%
# print('============= approximate + pruning group + pruning traj ==================')
# print('============= KM assignment ==================')
# value = []
# for i in Assignment_approximate_pruning.keys():
#     for j in range(1, len(greedy_group) + 1):
#         if j in Assignment_approximate_pruning[i].keys():
#             value.append((i, j, len(Assignment_approximate_pruning[i][j]['schedule'])))
#         else:
#             value.append((i, j, 0))
# total, assignment = run_kuhn_munkres(value)

# print('total:', total)

# total_cost = 0
# for pair in assignment:
#     if pair[-1] != 0:
#         wi = pair[0]
#         gj = pair[1]
#         # print(pair)
#         single_cost = single_pair_cost(wi, gj, Assignment_approximate_pruning, TaskPoint, WorkerTrajectory, LocationTrajectoryPoint)
#         total_cost += single_cost

# print('total cost:', total_cost, 'average cost:', total_cost/total)

# %%
print('============ approximate + pruning group + pruning traj + parallel ============')

average_time = 0
for round in range(rounds):
    start_time = time.time()
    Assignment_approximate_pruning_parallel = approximate_pruning_group_traj_parallel(greedy_group, WorkerTrajectory)
    greedy_assignment(Assignment_approximate_pruning_parallel)
    # print('schedule time:', time.time() - start_time)
    average_time += time.time() - start_time

average_time /= rounds
print('average time:', average_time)


# %%
print('============= PNN algorithm ==================')

average_time = 0
for round in range(rounds):
    start_time = time.time()
    pnn_schedule = pnn_baseline(detour_rate, group_size, worker_speed, TaskPoint, WorkerTrajectory, LocationTrajectoryPoint)
    print('time cost:', time.time() - start_time)
    # print('schedule time:', time.time() - start_time)
    average_time += time.time() - start_time

average_time /= rounds
print('average time:', average_time)


total_detour = 0
num = 0
for j in pnn_schedule.keys():
    num += len(pnn_schedule[j].keys())
    if len(pnn_schedule[j]) != 0:
        for t in pnn_schedule[j].keys():
            total_detour += get_distance_hav(TaskPoint[t]['location'], LocationTrajectoryPoint[pnn_schedule[j][t]['traj']]) * 2
print('num:', num, 'total cost:', total_detour, 'average cost:', (total_detour/num) * 1000, '(m)')



# %%
print('============= Exact algorithm ==================')

start_time = time.time()
exact_assignment = exact_algorithm(greedy_group, detour_rate, TaskPoint, WorkerTrajectory, LocationTrajectoryPoint)
print('schedule time:', time.time() - start_time)
print('============= greedy assignment =======================')
exact_start = time.time()
greedy_assignment(exact_assignment)
print('time cost:', time.time() - start_time)
