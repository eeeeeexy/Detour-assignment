import math, time, copy, json
from copy import deepcopy
from itertools import permutations
# from Grouping import Graph
import numpy as np
from math import sin, asin, cos, radians, fabs, sqrt
from collections import defaultdict

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

# def Euclidean_fun(A, B):
#     return math.sqrt(sum([(a - b)**2 for (a, b) in zip(A, B)]))

def task_insert(j, optimal_task, min_traj, k, pnn_schedule):
    # print(j, pnn_schedule[j])
    flag = True

    value_index = []
    for v in pnn_schedule[j].keys():
        value_index.append(WorkerTrajectory[j]['Trajectory'].index(pnn_schedule[j][v]['traj']))

    min_j = WorkerTrajectory[j]['Trajectory'].index(min_traj)
    # print(min_j, value_index)

    if len(pnn_schedule[j]) == 0:

        dist_seg = 0
        for k in range(0, min_j - 1):
            l1 = WorkerTrajectory[j]['Trajectory'][k]
            l2 = WorkerTrajectory[j]['Trajectory'][k + 1]
            dist_seg += get_distance_hav(LocationTrajectoryPoint[l1], LocationTrajectoryPoint[l2])
        dist_seg += get_distance_hav(LocationTrajectoryPoint[WorkerTrajectory[j]['Trajectory'][min_j]],
                                     TaskPoint[optimal_task]['location'])
        if dist_seg / worker_speed > TaskPoint[optimal_task]['expiration']:
            flag = False
        else:
            pnn_schedule[j][optimal_task] = {}
            pnn_schedule[j][optimal_task]['traj'] = min_traj
            pnn_schedule[j][optimal_task]['execution'] = dist_seg / worker_speed

    elif len(pnn_schedule[j].keys()) < k:

        # condition 1
        if min_j >= max(value_index):

            extra_detour = 0
            for t in pnn_schedule[j].keys():
                extra_detour += get_distance_hav(LocationTrajectoryPoint[pnn_schedule[j][t]['traj']], TaskPoint[t]['location']) * 2

            dist_seg = 0
            for k in range(0, min_j - 1):
                l1 = WorkerTrajectory[j]['Trajectory'][k]
                l2 = WorkerTrajectory[j]['Trajectory'][k + 1]
                dist_seg += get_distance_hav(LocationTrajectoryPoint[l1], LocationTrajectoryPoint[l2])
            dist_t2traj = get_distance_hav(LocationTrajectoryPoint[min_traj], TaskPoint[optimal_task]['location'])

            if (dist_seg + dist_t2traj) / worker_speed > TaskPoint[optimal_task]['expiration']:
                flag = False
            else:
                pnn_schedule[j][optimal_task] = {}
                pnn_schedule[j][optimal_task]['traj'] = min_traj
                pnn_schedule[j][optimal_task]['execution'] = (dist_seg + extra_detour + dist_t2traj) / worker_speed

        # condition 2
        elif min_j <= min(value_index):

            extra_detour = 0
            # 检测自身是否符合要求
            dist_seg = 0
            for k in range(0, min_j - 1):
                l1 = WorkerTrajectory[j]['Trajectory'][k]
                l2 = WorkerTrajectory[j]['Trajectory'][k + 1]
                dist_seg += get_distance_hav(LocationTrajectoryPoint[l1], LocationTrajectoryPoint[l2])
            dist_t2traj = get_distance_hav(LocationTrajectoryPoint[min_traj],
                                         TaskPoint[optimal_task]['location'])
            if (dist_seg + dist_t2traj) / worker_speed > TaskPoint[optimal_task]['expiration']:
                flag = False
            else:
                # 检测后面的task deadline是否符合要求
                flag_t = True
                extra_detour = dist_t2traj * 2
                for t in pnn_schedule[j].keys():
                    if extra_detour/worker_speed + pnn_schedule[j][t]['execution'] > TaskPoint[t]['expiration']:
                        flag_t = False

                if flag_t == False:
                    flag = False
                else:
                    # 插入这个节点
                    for t in pnn_schedule[j].keys():
                        pnn_schedule[j][t]['execution'] = pnn_schedule[j][t]['execution'] + extra_detour / worker_speed
                    # 更新其他节点
                    pnn_schedule[j][optimal_task] = {}
                    pnn_schedule[j][optimal_task]['traj'] = min_traj
                    pnn_schedule[j][optimal_task]['execution'] = (dist_seg + dist_t2traj) / worker_speed

        # condition 3
        else:
            flag_3 = True

            extra_detour_min = 0

            dist_seg = 0
            for k in range(0, min_j - 1):
                l1 = WorkerTrajectory[j]['Trajectory'][k]
                l2 = WorkerTrajectory[j]['Trajectory'][k + 1]
                dist_seg += get_distance_hav(LocationTrajectoryPoint[l1], LocationTrajectoryPoint[l2])
            dist_t2traj = get_distance_hav(LocationTrajectoryPoint[min_traj], TaskPoint[optimal_task]['location'])

            if (dist_seg + dist_t2traj) / worker_speed > TaskPoint[optimal_task]['expiration']:
                flag = False
            else:
                for t in pnn_schedule[j].keys():
                    extra_detour = dist_t2traj * 2
                    if min_j < WorkerTrajectory[j]['Trajectory'].index(pnn_schedule[j][t]['traj']):
                        if pnn_schedule[j][t]['execution'] + extra_detour/worker_speed > TaskPoint[t]['expiration']:
                            flag_3 = False
                    else:
                        extra_detour_min += get_distance_hav(LocationTrajectoryPoint[pnn_schedule[j][t]['traj']],
                                                TaskPoint[t]['location']) * 2
                if flag_3 == False:
                    flag = False
                else:
                    if (dist_seg + extra_detour_min + dist_t2traj) / worker_speed > TaskPoint[optimal_task]['expiration']:
                        flag = False
                    else:
                        pnn_schedule[j][optimal_task] = {}
                        pnn_schedule[j][optimal_task]['traj'] = min_traj
                        pnn_schedule[j][optimal_task]['execution'] = (dist_seg + extra_detour_min + dist_seg) / worker_speed

    return flag, pnn_schedule

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

if __name__ == '__main__':

    # # Beijing tdrive data
    # file1 = 'task_oslo.json'
    # file2 = 'LocationTrajectory_oslo.json'
    # file3 = 'worker_oslo.json'

    # # # Berlin data
    # # file1 = 'task-berlin.json'
    # # file2 = 'location-berlin.json'
    # # file3 = 'worker-berlin.json'

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
    group_size = 5
    group_range = 8
    detour_rate = 1
    dt = 4
    worker_speed = 60
    print('size:', group_size, 'range:', group_range, 'rate:', detour_rate)
    print('time:', dt, 'speed:', worker_speed)

    print('worker number:', len(WorkerTrajectory.keys()), 'trajectory number:', len(LocationTrajectoryPoint.keys()))
    print('task number:', len(TaskPoint.keys()))

    print('----- pnn baseline ------')
    start_time = time.time()
    global_temp_task = list(TaskPoint.keys())
    pnn_schedule = {}
    for j in WorkerTrajectory.keys():
        single_temp_task = copy.deepcopy(global_temp_task)
        pnn_schedule[j] = {}
        tao = detour_rate * WorkerTrajectory[j]['distance']
        while tao > 0:
            optimal_task = -1
            min_traj = -1
            min_distraj2task = 1000
            for traj in WorkerTrajectory[j]['Trajectory']:
                for t in single_temp_task:
                    dis_traj2task = get_distance_hav(LocationTrajectoryPoint[traj], TaskPoint[t]['location'])
                    if dis_traj2task <= 0.5 * tao:
                        if dis_traj2task < min_distraj2task:
                            min_distraj2task = dis_traj2task
                            min_traj = traj
                            optimal_task = t

            if tao > min_distraj2task:
                flag, pnn_insert = task_insert(j, optimal_task, min_traj, group_size, pnn_schedule)
                # flag, pnn_schedule = deadline_check(j, optimal_task, min_traj, pnn_schedule)
                if flag == False:
                    single_temp_task.remove(optimal_task)
                    continue
                else:
                    tao -= min_distraj2task * 2
                    single_temp_task.remove(optimal_task)
                    global_temp_task.remove(optimal_task)
                    pnn_schedule = pnn_insert
                    # print(j, optimal_task, len(pnn_schedule[j].keys()))
            if tao < min_distraj2task:
                break
            # print(j, min_traj, optimal_task, tao)

    # print(pnn_schedule)
    print(time.time() - start_time)

    total_detour = 0
    assignment = 0
    for j in pnn_schedule.keys():
        assignment += len(pnn_schedule[j].keys())
        if len(pnn_schedule[j]) != 0:
            for t in pnn_schedule[j].keys():
                total_detour += get_distance_hav(TaskPoint[t]['location'], LocationTrajectoryPoint[pnn_schedule[j][t]['traj']]) * 2
    print(total_detour, assignment)

    # for p in pnn_schedule.keys():
    #     print(p, pnn_schedule[p].keys())
