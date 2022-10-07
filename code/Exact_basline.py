from utils import get_distance_hav, worker_seg_traj_distance
from itertools import permutations

from utils import task_deadline_check

worker_speed = 60

def detour_pair(worker_trajectory):
    detour_combin = []
    for i in range(0, len(worker_trajectory) - 1):
        for j in range(i + 1, len(worker_trajectory)):
            detour_combin.append([worker_trajectory[i], worker_trajectory[j]])

    return detour_combin


def group_travel_cost(group_list, TaskPoint):
    if len(group_list) > 1:
        dist_sum = 0
        for i in range(0, len(group_list) - 1):
            dist_sum += get_distance_hav(TaskPoint[group_list[i] + 1]['location'], TaskPoint[group_list[i + 1] + 1]['location'])
        return dist_sum
    else:
        return 0


def exact_algorithm(group_greedy, detour_rate, TaskPoint, WorkerTrajectory, LocationTrajectoryPoint):

    Assignment = {}
    for j in WorkerTrajectory.keys():
    # for j in range(31, 32):

        tao = detour_rate * WorkerTrajectory[j]['distance']

        Assignment[j] = {}

        detour_pair_combine = detour_pair(WorkerTrajectory[j]['Trajectory'])

        group_id = 0
        for group_list in group_greedy:
            group_id += 1

            # 对 group中的所有任务排列组合
            combin_groups = list(permutations(group_list, len(group_list)))

            # 遍历所有任务组合情况, 找到最小成本花费的任务规划
            min_dist = 999
            min_cost = 999
            best_schedule = []
            best_pair = []
            id = 0

            for p in combin_groups:
                group = list(p)
                dist_0 = group_travel_cost(group, TaskPoint)
                # print(j, group_list, group, dist_0, dist_task_schedule(group, group[-1]))

                # 遍历所有轨迹上的偏移对
                if dist_0 < tao:
                    # print(group_id, ':', p, dist_0)

                    dist_group = 0
                    for pair in detour_pair_combine:

                        # print('pair:', pair)

                        # 绕路成本，从偏移对到group的距离以及group内部的移动距离
                        dist_1 = dist_0 + get_distance_hav(
                            TaskPoint[group[0] + 1]['location'], LocationTrajectoryPoint[pair[0]]) + get_distance_hav(
                            TaskPoint[group[-1] + 1]['location'], LocationTrajectoryPoint[pair[-1]])

                        # 如果满足绕路限制，参与最佳的轨迹规划的最优比较
                        if dist_1 <= tao:
                            #  and task_deadline_check(group_list, j, pair, worker_speed, TaskPoint, WorkerTrajectory, LocationTrajectoryPoint) == True:

                            dist_group = dist_1 - worker_seg_traj_distance(j, WorkerTrajectory[j]['Trajectory'].index(
                                pair[0]), WorkerTrajectory[j]['Trajectory'].index(pair[-1]), WorkerTrajectory, LocationTrajectoryPoint)

                            # 比较所有组合，寻找最优的轨迹规划
                            if dist_group < min_dist:
                                # min_cost = dist_1
                                min_dist = dist_group
                                best_schedule = list(p)
                                best_pair = pair
                                id = group_id

            if len(best_schedule) != 0:
                Assignment[j][id] = {}
                Assignment[j][id]['schedule'] = best_schedule
                Assignment[j][id]['detour'] = best_pair
                Assignment[j][id]['cost'] = min_dist

    return Assignment
