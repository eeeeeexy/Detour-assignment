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

from utils import MST, DFS, dist_task_schedule

from Exact_basline import exact_algorithm

from My_methods import approximate_algorithm, approximate_pruning_group, approximate_pruning_group_traj


worker_speed = 60
capacity = 5
detour_rate = 0.7


# def greedy_algorithm(f):

#     with open(f, 'r') as f_greedy:
#         assignment_1 = json.load(f_greedy)
#     greedy_assignment = {}
#     for k in assignment_1.keys():
#         p = int(k)
#         greedy_assignment[p] = {}
#         for kk in assignment_1[k].keys():
#             pp = int(kk)
#             greedy_assignment[p][pp] = assignment_1[k][kk]
#     # print('Candidate assignment:', greedy_assignment.keys())

#     total_cost = 0
#     assign_groups = []
#     for i in greedy_assignment.keys():
#         group_id = -1
#         max_group = 0
#         min_cost = 99
#         for j in greedy_assignment[i].keys():
#             if j not in assign_groups:
#                 if len(greedy_assignment[i][j]['schedule']) > max_group:
#                     max_group = len(greedy_assignment[i][j])
#                     if greedy_assignment[i][j]['cost'] < min_cost:
#                         min_cost = greedy_assignment[i][j]['cost']
#                         group_id = j
#         if group_id != -1:
#             assign_groups.append(group_id)
#             total_cost += greedy_assignment[i][group_id]['cost']
#     # print('assigned groups:', len(assign_groups), assign_groups)
#     total_tasks = 0
#     for t in assign_groups:
#         total_tasks += len(greedy_group[t - 1])

#     print('assigned tasks:', total_tasks)

#     return total_cost, assign_groups, greedy_assignment


# def approximate_algorithm(group_greedy):
#     Assignment = {}

#     for j in WorkerTrajectory.keys():
#     # for j in range(31, 32):

#         # worker 轨迹上的所有轨迹对组合
#         worker_traj_pair_combine = detour_pair(WorkerTrajectory[j]['Trajectory'])
#         # 第 j 个 worker的绕路总距离
#         tao = detour_rate * WorkerTrajectory[j]['distance']

#         Assignment[j] = {}
#         group_id = 0
#         for group_list in group_greedy:
#             group_id += 1

#             flag = True

#             if flag == False:
#                 continue

#             else:
#                 # 当 group的规模超过一个 task
#                 if len(group_list) > 1:

#                     # 采用近似算法生成group和轨迹点的最小生成树
#                     spanning_tree, flag_tree = MST(group_list, WorkerTrajectory[j]['Trajectory'], TaskPoint, LocationTrajectoryPoint)
#                     # 应用 DFS得到 group完成的闭环
#                     task_schedule_circle = DFS(spanning_tree, flag_tree, group_list[0])

#                     optimal_detour_cost = 99  # 不减轨迹段
#                     optimal_start_task = -1
#                     optimal_pair = []

#                     # 遍历group的组合
#                     for s in range(1, len(task_schedule_circle)):

#                         start_task = task_schedule_circle[s]
#                         end_task = task_schedule_circle[s - 1]

#                         # group 内部完成所有任务的总距离
#                         dist_2 = dist_task_schedule(task_schedule_circle, end_task, TaskPoint)

#                         min_detour_cost = 99  # 不减去轨迹段的 cost

#                         for pair in worker_traj_pair_combine:

#                             # 偏移对到 group的两段距离
#                             dist_1 = get_distance_hav(LocationTrajectoryPoint[pair[0]], TaskPoint[start_task + 1]['location'])
#                             dist_3 = get_distance_hav(LocationTrajectoryPoint[pair[-1]], TaskPoint[end_task + 1]['location'])

#                             if dist_1 + dist_2 + dist_3 <= tao:
#                                 available_cost = dist_1 + dist_2 + dist_3
#                                 available_pair = pair

#                                 if available_cost - worker_seg_traj_distance(j, WorkerTrajectory[j]['Trajectory'].index(
#                                         available_pair[0]), WorkerTrajectory[j]['Trajectory'].index(available_pair[-1]), WorkerTrajectory, LocationTrajectoryPoint) < optimal_detour_cost and task_deadline_check(group_list, j, available_pair, worker_speed, TaskPoint, WorkerTrajectory, LocationTrajectoryPoint) == True:

#                                     optimal_detour_cost = available_cost - worker_seg_traj_distance(j, WorkerTrajectory[j]['Trajectory'].index(
#                                         available_pair[0]), WorkerTrajectory[j]['Trajectory'].index(available_pair[-1]), WorkerTrajectory, LocationTrajectoryPoint)
#                                     optimal_start_task = start_task
#                                     optimal_pair = available_pair

#                     if optimal_start_task != -1:

#                         left_list = []
#                         right_list = []
#                         for i in range(0, len(task_schedule_circle)):
#                             if task_schedule_circle.index(task_schedule_circle[i]) >= task_schedule_circle.index(
#                                     optimal_start_task):
#                                 right_list.append(task_schedule_circle[i])
#                             else:
#                                 left_list.append(task_schedule_circle[i])
#                         right_list.extend(left_list)

#                         Assignment[j][group_id] = {}
#                         Assignment[j][group_id]['schedule'] = right_list
#                         Assignment[j][group_id]['detour'] = optimal_pair
#                         Assignment[j][group_id]['cost'] = optimal_detour_cost

#                 # 当 group中只有一个任务
#                 if len(group_list) == 1:
#                     task_center = group_list[0]

#                     # 遍历轨迹点从左到右
#                     near_dist = 99

#                     # 找到离任务最近的轨迹点
#                     for oo in range(0, len(WorkerTrajectory[j]['Trajectory'])):

#                         dist_4 = get_distance_hav(LocationTrajectoryPoint[WorkerTrajectory[j]['Trajectory'][oo]],
#                                             TaskPoint[task_center + 1]['location'])
#                         if dist_4 < near_dist:
#                             near_dist = dist_4

#                     # 根据绕路限制，筛选可达的轨迹点
#                     candidate_traj = []
#                     for o in range(0, len(WorkerTrajectory[j]['Trajectory'])):
#                         if get_distance_hav(LocationTrajectoryPoint[WorkerTrajectory[j]['Trajectory'][o]],
#                                         TaskPoint[task_center + 1]['location']) < tao - near_dist:
#                             candidate_traj.append(o)

#                     # 从左和右同时相对遍历，一旦有轨迹对符合绕路限制，停止遍历，返回轨迹对
#                     best_detour_pair = []
#                     optimal_cost = 99
#                     optimal_detour_cost = 99
#                     for l in range(0, len(candidate_traj)):
#                         for r in range(len(candidate_traj) - 1, l, -1):
#                             dist_5 = get_distance_hav(
#                                 LocationTrajectoryPoint[WorkerTrajectory[j]['Trajectory'][candidate_traj[l]]],
#                                 TaskPoint[task_center + 1]['location']) + get_distance_hav(
#                                 LocationTrajectoryPoint[WorkerTrajectory[j]['Trajectory'][candidate_traj[r]]],
#                                 TaskPoint[task_center + 1]['location'])
#                             if dist_5 < tao:
                                
#                                 # print('pair:', l, r, dist_5, dist_5 - worker_seg_traj_distance(j, l, r))
#                                 best_detour_pair = [WorkerTrajectory[j]['Trajectory'][candidate_traj[l]],
#                                                     WorkerTrajectory[j]['Trajectory'][candidate_traj[r]]]
#                                 optimal_cost = dist_5 - worker_seg_traj_distance(j, candidate_traj[l],
#                                                                                 candidate_traj[r], WorkerTrajectory, LocationTrajectoryPoint)
#                                 # optimal_detour_cost = dist_5
#                                 break
#                         break

#                     if len(candidate_traj) > 1 and len(best_detour_pair) > 0:
#                         Assignment[j][group_id] = {}
#                         Assignment[j][group_id]['schedule'] = [task_center]
#                         Assignment[j][group_id]['detour'] = best_detour_pair
#                         Assignment[j][group_id]['cost'] = optimal_cost
    
#     return Assignment


# def approximate_pruning_group(group_greedy):
#     Assignment = {}

#     for j in WorkerTrajectory.keys():
#     # for j in range(31, 32):

#         tao = detour_rate * WorkerTrajectory[j]['distance']
#         # print('trajectory:', WorkerTrajectory[j]['Trajectory'], tao)
#         Assignment[j] = {}
#         group_id = 0
#         for group_list in group_greedy:
#             group_id += 1

#             # 对 group 剪枝
#             flag = True
#             for i in group_list:
#                 if dist_task_traj(i, WorkerTrajectory[j]['Trajectory'], TaskPoint, LocationTrajectoryPoint)[-1] > 0.5 * tao:
#                     flag = False
#                     break

#             if flag == False:
#                 continue

#             # 当 group 合格
#             else:

#                 spanning_tree, flag_tree = MST(group_list, WorkerTrajectory[j]['Trajectory'], TaskPoint, LocationTrajectoryPoint)
#                 task_schedule_circle = DFS(spanning_tree, flag_tree, group_list[0])

#                 # 当 group的规模超过一个 task
#                 if len(task_schedule_circle) > 1:

#                     # 遍历group的组合种类
#                     optimal_start_traj = -1
#                     optimal_end_traj = -1
#                     optimal_cost = 99  # 减去轨迹段
#                     optimal_detour_cost = 99  # 不减轨迹段
#                     optimal_start_task = -1

#                     for s in range(1, len(task_schedule_circle)):

#                         start_task = task_schedule_circle[s]
#                         end_task = task_schedule_circle[s - 1]

#                         # group 内部完成所有任务的总距离
#                         dist_2 = dist_task_schedule(task_schedule_circle, end_task, TaskPoint)

#                         min_start_traj = -1
#                         min_end_traj = -1
#                         min_cost_cost = 99  # 减去轨迹段的 cost
#                         min_detour_cost = 99  # 不减去轨迹段的 cost

#                         # 遍历轨迹点从左到右
#                         for o in range(0, len(WorkerTrajectory[j]['Trajectory']) - 1):

#                             min_cost = 99
#                             # 偏出轨迹点离任务组的起始任务的距离
#                             dist_1 = get_distance_hav(LocationTrajectoryPoint[WorkerTrajectory[j]['Trajectory'][o]],
#                                                    TaskPoint[start_task + 1]['location'])

#                             # 遍历轨迹点从右到左
#                             start_traj = -1
#                             end_traj = -1
#                             for sss in range(len(WorkerTrajectory[j]['Trajectory'])-1, o, -1):
#                                 # 尾部任务离右侧遍历轨迹点的距离
#                                 dist_3 = get_distance_hav(
#                                     LocationTrajectoryPoint[WorkerTrajectory[j]['Trajectory'][sss]],
#                                     TaskPoint[end_task + 1]['location'])
#                                 # 一旦从最右侧起找到符合限制的偏入轨迹点，则停止，根据引理可以知道绕路最小
#                                 if dist_1 + dist_2 + dist_3 <= tao:

#                                     min_cost = dist_1 + dist_2 + dist_3
#                                     start_traj = o
#                                     end_traj = sss
#                                     break

#                             if start_traj != -1 and min_cost - worker_seg_traj_distance(j, start_traj, end_traj, WorkerTrajectory, LocationTrajectoryPoint) < min_cost_cost \
#                              and task_deadline_check(group_list, j, [WorkerTrajectory[j]['Trajectory'][start_traj], WorkerTrajectory[j]['Trajectory'][end_traj]], worker_speed, TaskPoint, WorkerTrajectory, LocationTrajectoryPoint) == True:
                                
#                                 min_cost_cost = min_cost - worker_seg_traj_distance(j, start_traj, end_traj, WorkerTrajectory, LocationTrajectoryPoint)
#                                 min_start_traj = start_traj
#                                 min_end_traj = end_traj
#                                 min_detour_cost = min_cost

#                         if min_cost_cost < optimal_cost:
#                             optimal_detour_cost = min_detour_cost
#                             optimal_start_traj = min_start_traj
#                             optimal_end_traj = min_end_traj
#                             optimal_start_task = start_task

#                     if optimal_start_task != -1:

#                         left_list = []
#                         right_list = []
#                         for i in range(0, len(task_schedule_circle)):
#                             if task_schedule_circle.index(task_schedule_circle[i]) >= task_schedule_circle.index(optimal_start_task):
#                                 right_list.append(task_schedule_circle[i])
#                             else:
#                                 left_list.append(task_schedule_circle[i])
#                         right_list.extend(left_list)

#                         Assignment[j][group_id] = {}
#                         Assignment[j][group_id]['schedule'] = right_list
#                         Assignment[j][group_id]['detour'] = [WorkerTrajectory[j]['Trajectory'][optimal_start_traj], WorkerTrajectory[j]['Trajectory'][optimal_end_traj]]
#                         Assignment[j][group_id]['cost'] = optimal_detour_cost

#                 # 当 group中只有一个任务
#                 if len(task_schedule_circle) == 1:
#                     task_center = task_schedule_circle[0]

#                     # 遍历轨迹点从左到右
#                     near_dist = 99

#                     # 找到离任务最近的轨迹点
#                     for oo in range(0, len(WorkerTrajectory[j]['Trajectory'])):

#                         dist_4 = get_distance_hav(LocationTrajectoryPoint[WorkerTrajectory[j]['Trajectory'][oo]], TaskPoint[task_center + 1]['location'])
#                         if dist_4 < near_dist:
#                             near_dist = dist_4

#                     # 根据绕路限制，筛选可达的轨迹点
#                     candidate_traj = []
#                     for o in range(0, len(WorkerTrajectory[j]['Trajectory'])):
#                         if get_distance_hav(LocationTrajectoryPoint[WorkerTrajectory[j]['Trajectory'][o]], TaskPoint[task_center + 1]['location']) < tao - near_dist:
#                             candidate_traj.append(o)

#                     # 从左和右同时相对遍历，一旦有轨迹对符合绕路限制，停止遍历，返回轨迹对
#                     best_detour_pair = []
#                     optimal_cost = 99
#                     optimal_detour_cost = 99
#                     for l in range(0, len(candidate_traj)):
#                         for r in range(len(candidate_traj)-1, l, -1):
#                             dist_5 = get_distance_hav(
#                                 LocationTrajectoryPoint[WorkerTrajectory[j]['Trajectory'][candidate_traj[l]]],
#                                 TaskPoint[task_center + 1]['location']) + get_distance_hav(
#                                 LocationTrajectoryPoint[WorkerTrajectory[j]['Trajectory'][candidate_traj[r]]],
#                                 TaskPoint[task_center + 1]['location'])
#                             if dist_5 < tao:

#                                 best_detour_pair = [WorkerTrajectory[j]['Trajectory'][candidate_traj[l]], WorkerTrajectory[j]['Trajectory'][candidate_traj[r]]]
#                                 optimal_cost = dist_5 - worker_seg_traj_distance(j, candidate_traj[l], candidate_traj[r], WorkerTrajectory, LocationTrajectoryPoint)
 
#                                 break
#                         break

#                     if len(candidate_traj) > 1 and len(best_detour_pair) > 0:
#                         Assignment[j][group_id] = {}
#                         Assignment[j][group_id]['schedule'] = [task_center]
#                         Assignment[j][group_id]['detour'] = best_detour_pair
#                         Assignment[j][group_id]['cost'] = optimal_cost

#     return Assignment


# def approximate_pruning_group_traj(group_greedy):

#     Assignment = {}

#     for j in WorkerTrajectory.keys():

#         tao = detour_rate * WorkerTrajectory[j]['distance']
#         # tao = 0.2

#         Assignment[j] = {}

#         group_id = 0

#         for group_list in group_greedy:
#             group_id += 1

#             flag = True
#             for i in group_list:
#                 if dist_task_traj(i, WorkerTrajectory[j]['Trajectory'], TaskPoint, LocationTrajectoryPoint)[-1] > 0.5 * tao:
#                     flag = False
#                     break

#             if flag == False:
#                 continue

#             else:

#                 if len(group_list) > 1:

#                     spanning_tree, flag_tree = MST(group_list, WorkerTrajectory[j]['Trajectory'], TaskPoint, LocationTrajectoryPoint)
#                     task_schedule_circle = DFS(spanning_tree, flag_tree, group_list[0])

#                     optimal_cost = 99
#                     optimal_start_task = -1
#                     optimal_detour = []

#                     near_dist = 99
#                     n_e = -1
#                     for t in task_schedule_circle:
#                         for o in range(0, len(WorkerTrajectory[j]['Trajectory'])):
#                             dist_n = get_distance_hav(LocationTrajectoryPoint[WorkerTrajectory[j]['Trajectory'][o]],
#                                                    TaskPoint[t + 1]['location'])
#                             if dist_n < near_dist:
#                                 near_dist = dist_n
#                                 # n_e = o

#                     for s in range(1, len(task_schedule_circle)):

#                         start_task = task_schedule_circle[s]
#                         end_task = task_schedule_circle[s - 1]

#                         dist_2 = dist_task_schedule(task_schedule_circle, end_task, TaskPoint)

#                         candidate_traj = []
#                         for o in range(0, len(WorkerTrajectory[j]['Trajectory'])):

#                             if get_distance_hav(LocationTrajectoryPoint[WorkerTrajectory[j]['Trajectory'][o]],
#                                 TaskPoint[start_task + 1]['location']) <= tao - near_dist - dist_2 or get_distance_hav(
#                                 LocationTrajectoryPoint[WorkerTrajectory[j]['Trajectory'][o]],
#                                 TaskPoint[end_task + 1]['location']) <= tao - near_dist - dist_2:

#                                 candidate_traj.append(o)

#                         best_detour_pair = []
#                         min_cost = 99
#                         for l in range(0, len(candidate_traj)):
#                             for r in range(len(candidate_traj) - 1, l, -1):
#                                 dist_5 = get_distance_hav(
#                                     LocationTrajectoryPoint[WorkerTrajectory[j]['Trajectory'][candidate_traj[l]]],
#                                     TaskPoint[start_task + 1]['location']) + get_distance_hav(
#                                     LocationTrajectoryPoint[WorkerTrajectory[j]['Trajectory'][candidate_traj[r]]],
#                                     TaskPoint[end_task + 1]['location']) + dist_2
#                                 if dist_5 < tao and task_deadline_check(group_list, j, [WorkerTrajectory[j]['Trajectory'][candidate_traj[l]],
#                                     WorkerTrajectory[j]['Trajectory'][candidate_traj[r]]], worker_speed, TaskPoint, WorkerTrajectory, LocationTrajectoryPoint) == True:
                                    
#                                     best_detour_pair = [WorkerTrajectory[j]['Trajectory'][candidate_traj[l]],
#                                                         WorkerTrajectory[j]['Trajectory'][candidate_traj[r]]]
#                                     min_cost = dist_5 - worker_seg_traj_distance(j, candidate_traj[l], candidate_traj[r], WorkerTrajectory, LocationTrajectoryPoint)
#                                     break
#                             break

#                         if min_cost < optimal_cost:
#                             optimal_cost = min_cost
#                             optimal_detour = best_detour_pair
#                             optimal_start_task = start_task

#                     if optimal_start_task != -1:

#                         left_list = []
#                         right_list = []
#                         for i in range(0, len(task_schedule_circle)):
#                             if task_schedule_circle.index(task_schedule_circle[i]) >= task_schedule_circle.index(
#                                     optimal_start_task):
#                                 right_list.append(task_schedule_circle[i])
#                             else:
#                                 left_list.append(task_schedule_circle[i])
#                         right_list.extend(left_list)

#                         Assignment[j][group_id] = {}
#                         Assignment[j][group_id]['schedule'] = right_list
#                         Assignment[j][group_id]['detour'] = optimal_detour

#                 if len(group_list) == 1:
#                     task_center = group_list[0]

#                     near_dist = 99

#                     n_e = -1
#                     for oo in range(0, len(WorkerTrajectory[j]['Trajectory'])):

#                         dist_4 = get_distance_hav(LocationTrajectoryPoint[WorkerTrajectory[j]['Trajectory'][oo]],
#                                                TaskPoint[task_center + 1]['location'])
#                         if dist_4 < near_dist:
#                             near_dist = dist_4
#                             n_e = oo

#                     candidate_traj = []
#                     for o in range(0, len(WorkerTrajectory[j]['Trajectory'])):
#                         if get_distance_hav(LocationTrajectoryPoint[WorkerTrajectory[j]['Trajectory'][o]],
#                                          TaskPoint[task_center + 1]['location']) < tao - near_dist:
#                             candidate_traj.append(o)

#                     best_detour_pair = []
#                     # optimal_cost = -1
#                     for l in range(0, len(candidate_traj)):
#                         for r in range(len(candidate_traj) - 1, l, -1):
#                             dist_5 = get_distance_hav(
#                                 LocationTrajectoryPoint[WorkerTrajectory[j]['Trajectory'][candidate_traj[l]]],
#                                 TaskPoint[task_center + 1]['location']) + get_distance_hav(
#                                 LocationTrajectoryPoint[WorkerTrajectory[j]['Trajectory'][candidate_traj[r]]],
#                                 TaskPoint[task_center + 1]['location'])
#                             if dist_5 < tao:
#                                 best_detour_pair = [WorkerTrajectory[j]['Trajectory'][candidate_traj[l]],
#                                                     WorkerTrajectory[j]['Trajectory'][candidate_traj[r]]]
#                                 break
#                         break

#                     if len(candidate_traj) > 1 and len(best_detour_pair) > 0:
#                         Assignment[j][group_id] = {}
#                         Assignment[j][group_id]['schedule'] = [task_center]
#                         Assignment[j][group_id]['detour'] = best_detour_pair

#     return Assignment


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

    result = [pool.apply_async(schedule_pair, args=(j, group_greedy )) for j in WorkerTrajectory.keys()]
    root = [r.get() for r in result]

    pool.close()
    pool.join()

    for dic in root:
        for key in dic:
            Assignment[key] = dic[key]

    return Assignment


def approximate_pruning_group_traj_series(group_greedy):

    Assignment = {}
    # series
    for j in WorkerTrajectory.keys():
        Assignment[j] = schedule_pair(j, group_greedy)[j]

    return Assignment


def schedule_assign(schedule, total_cost, total, assignment):

    # # 邻接矩阵版本
    # first_start = time.time()
    # value = []
    # for i in schedule.keys():
    #     for j in range(1, len(greedy_group) + 1):
    #         if j in schedule[i].keys():
    #             value.append((i, j, len(schedule[i][j]['schedule'])))
    #         else:
    #             value.append((i, j, 0))
    # total, assignment = run_kuhn_munkres(value)
    # total_cost = real_distance(schedule, assignment)
    # print(total, total_cost, len(assignment), 'initial assignment time cost:', time.time() - first_start)
    # print('===============')

    min_cost = total_cost
    optimal_assignment = []

    total_new = total
    while total == total_new:

        total_temp = total
        # 在匹配结果中找到 cost最大的 pair
        max_cost = -1000
        max_wi = -1
        max_gt = -1

        for m in assignment:
            if m[-1] != 0 and real_distance_group(schedule, m[0], m[1], TaskPoint, WorkerTrajectory, LocationTrajectoryPoint) > max_cost:
                max_cost = real_distance_group(schedule, m[0], m[1], TaskPoint, WorkerTrajectory, LocationTrajectoryPoint)
                max_wi = m[0]
                max_gt = m[1]

        # 将 value中的 cost最大的 pair设置为 0
        maxx_final = []
        for maxx in value:
            if maxx[0] == max_wi and maxx[1] == max_gt:
                maxx = list(maxx)
                maxx.remove(maxx[-1])
                maxx.append(0)
                maxx_final = tuple(maxx)
        if len(maxx_final) != 0:
            value.remove((maxx_final[0], maxx_final[1], len(schedule[maxx_final[0]][maxx_final[1]]['schedule'])))
            value.append(maxx_final)

        # 将 value中大于 cost的边全部设为0
        for val_copy in value:
            val = val_copy
            if val[-1] != 0 and real_distance_group(schedule, val[0], val[1], TaskPoint, WorkerTrajectory, LocationTrajectoryPoint) > max_cost:
                value.remove(val)
                val = list(val)
                val.remove(val[-1])
                val.append(0)
                val = tuple(val)
                value.append(val)

        # 重新进行匹配
        total, assignment = run_kuhn_munkres(value)
        total_cost = real_distance(schedule, assignment, TaskPoint, WorkerTrajectory, LocationTrajectoryPoint)
        if total == total_temp and total_cost < min_cost:
            optimal_assignment = assignment
            min_cost = total_cost
        print(total, total_cost, len(assignment))
        total_new = total_temp

    print('final optimal assignment cost:', min_cost, len(optimal_assignment))
            
    




if __name__ == '__main__':

    f = open("SFAL_log_wo_deadline.txt", "a")

    dw = 3
    dt = 2
    worker_speed = 60

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
    group_size = 5
    group_range = 0.4
    detour_rate = 0.7
    spanning_alpha = 0.0001
    print('group size:', group_size, 'group_range:', group_range)

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

    # count = 0
    # for i in range(0, len(greedy_group_result)):
    #     count += len(greedy_group_result[i])
    # print('greedy count:', count)

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

    # print('======= DBSCAN grouping =========')

    # dbscan_start_time = time.time()
    # DBSCAN_grouping = []
    # task_datas = []
    # for task in TaskPoint.keys():
    #     task_datas.append(TaskPoint[task]['location'])
    # print('task datas:', len(task_datas))

    # X = np.array(task_datas)

    # db = skc.DBSCAN(eps=0.004, min_samples=1).fit(X) #DBSCAN聚类方法 还有参数，matric = ""距离计算方法
    # labels = db.labels_  #和X同一个维度，labels对应索引序号的值 为她所在簇的序号。若簇编号为-1，表示为噪声

    # # print('每个样本的簇标号:')
    # # print(labels)

    # raito = len(labels[labels[:] == -1]) / len(labels)  #计算噪声点个数占总数的比例

    # n_clusters_ = len(set(labels)) - (1 if -1 in labels else 0)  # 获取分簇的数目

    # print('分簇的数目: %d' % n_clusters_)

    # for i in range(n_clusters_):
    #     one_cluster = X[labels == i]
    #     label_index = []
    #     for n in one_cluster:
    #         label_index.append(np.where(X == n)[0][0])
    #     DBSCAN_grouping.append(label_index)

    # print('dbscan time:', time.time() - dbscan_start_time)

    print('====== grouping result =====')
    greedy_group = greedy_group_result
    # greedy_group = Group_spanning
    # greedy_group = DBSCAN_grouping

    # print('============ approximate ============', file=f)
    # start_time = time.time()
    # schedule_a = approximate_algorithm(greedy_group, TaskPoint, WorkerTrajectory, LocationTrajectoryPoint)
    # print('schedule time:', time.time() - start_time, file=f)

    # value = []
    # for i in schedule_a.keys():
    #     for j in range(1, len(greedy_group) + 1):
    #         if j in schedule_a[i].keys():
    #             value.append((i, j, len(schedule_a[i][j]['schedule'])))
    #         else:
    #            value.append((i, j, 0))
    # total, assignment = run_kuhn_munkres(value)
    # total_cost = real_distance(schedule_a, assignment, TaskPoint, WorkerTrajectory, LocationTrajectoryPoint)
    # print('approximate group time:', time.time() - start_time, 'total:', total, 'total cost:', total_cost, file=f)

    # print('============ approximate + pruning group ============', file=f)
    # start_time = time.time()
    # schedule = approximate_pruning_group(greedy_group, TaskPoint, WorkerTrajectory, LocationTrajectoryPoint)
    # print('schedule time:', time.time() - start_time, file=f)
    
    # value = []
    # for i in schedule.keys():
    #     for j in range(1, len(greedy_group) + 1):
    #         if j in schedule[i].keys():
    #             value.append((i, j, len(schedule[i][j]['schedule'])))
    #         else:
    #             value.append((i, j, 0))
    # total, assignment = run_kuhn_munkres(value)
    # total_cost = real_distance(schedule, assignment, TaskPoint, WorkerTrajectory, LocationTrajectoryPoint)
    # print('approximate group time:', time.time() - start_time, 'total:', total, 'total cost:', total_cost, file=f)


    print('============ approximate + pruning group + pruning traj ============', file=f)
    start_time = time.time()
    Assignment_approximate_pruning = approximate_pruning_group_traj(greedy_group, TaskPoint, WorkerTrajectory, LocationTrajectoryPoint)
    print('schedule time:', time.time() - start_time, file=f)

    # greedy_assignment(Assignment_approximate_pruning)


    value = []
    for i in Assignment_approximate_pruning.keys():
        for j in range(1, len(greedy_group) + 1):
            if j in Assignment_approximate_pruning[i].keys():
                value.append((i, j, len(Assignment_approximate_pruning[i][j]['schedule'])))
            else:
                value.append((i, j, 0))
    total, assignment = run_kuhn_munkres(value)
    total_cost = real_distance(Assignment_approximate_pruning, assignment, TaskPoint, WorkerTrajectory, LocationTrajectoryPoint)
    print('approximate group traj time:', time.time() - start_time, total, total_cost, file=f)

    # print('============ approximate + pruning group + pruning traj + parallel ============', file=f)
    # start_time = time.time()
    # Assignment_approximate_pruning_parallel = approximate_pruning_group_traj_parallel(greedy_group, WorkerTrajectory)
    # print('schedule time:', time.time() - start_time, file=f)

    # value = []
    # for i in Assignment_approximate_pruning_parallel.keys():
    #     for j in range(1, len(greedy_group) + 1):
    #         if j in Assignment_approximate_pruning_parallel[i].keys():
    #             value.append((i, j, len(Assignment_approximate_pruning_parallel[i][j]['schedule'])))
    #         else:
    #             value.append((i, j, 0))
    # total, assignment = run_kuhn_munkres(value)
    # total_cost = real_distance(Assignment_approximate_pruning_parallel, assignment, TaskPoint, WorkerTrajectory, LocationTrajectoryPoint)
    # print('approximate group traj time:', time.time() - start_time, file=f)

    # print('============= Exact algorithm ==================', file=f)
    # start_time = time.time()
    # exact_assignment = exact_algorithm(greedy_group, detour_rate, TaskPoint, WorkerTrajectory, LocationTrajectoryPoint)
    # print('schedule time:', time.time() - start_time, file=f)

    # value = []
    # for i in exact_assignment.keys():
    #     for j in range(1, len(greedy_group) + 1):
    #         if j in exact_assignment[i].keys():
    #             value.append((i, j, len(exact_assignment[i][j]['schedule'])))
    #         else:
    #             value.append((i, j, 0))
    # total, assignment = run_kuhn_munkres(value)
    # total_cost = real_distance(exact_assignment, assignment, TaskPoint, WorkerTrajectory, LocationTrajectoryPoint)
    # print('approximate group traj time:', time.time() - start_time, total, total_cost, file=f)

    # value = []
    # for i in exact_assignment.keys():
    #     for j in range(1, len(greedy_group) + 1):
    #         if j in exact_assignment[i].keys():
    #             value.append((i, j, len(exact_assignment[i][j]['schedule'])))
    #         else:
    #             value.append((i, j, 0))
    # total, assignment = run_kuhn_munkres(value)
    # total_cost = real_distance(exact_assignment, assignment, TaskPoint, WorkerTrajectory, LocationTrajectoryPoint)
    # print(total, total_cost, len(assignment), file=f)
    # print('approximate group traj time:', time.time() - start_time, file=f)

    # exact_optimal_assignment = schedule_assign(exact_assignment, total, total_cost, assignment)
    # print('total time:', time.time() - start_time, file=f)

    # f.close()