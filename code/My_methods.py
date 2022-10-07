from utils import get_distance_hav
from utils import worker_seg_traj_distance
from utils import dist_task_traj
from utils import detour_pair
from utils import task_deadline_check

from utils import run_kuhn_munkres
from utils import real_distance, real_distance_group

from utils import MST, DFS, dist_task_schedule

import multiprocessing, json


capacity = 5

def greedy_algorithm(f, greedy_group):

    with open(f, 'r') as f_greedy:
        assignment_1 = json.load(f_greedy)
    greedy_assignment = {}
    for k in assignment_1.keys():
        p = int(k)
        greedy_assignment[p] = {}
        for kk in assignment_1[k].keys():
            pp = int(kk)
            greedy_assignment[p][pp] = assignment_1[k][kk]
    # print('Candidate assignment:', greedy_assignment.keys())

    total_cost = 0
    assign_groups = []
    for i in greedy_assignment.keys():
        group_id = -1
        max_group = 0
        min_cost = 99
        for j in greedy_assignment[i].keys():
            if j not in assign_groups:
                if len(greedy_assignment[i][j]['schedule']) > max_group:
                    max_group = len(greedy_assignment[i][j])
                    if greedy_assignment[i][j]['cost'] < min_cost:
                        min_cost = greedy_assignment[i][j]['cost']
                        group_id = j
        if group_id != -1:
            assign_groups.append(group_id)
            total_cost += greedy_assignment[i][group_id]['cost']
    # print('assigned groups:', len(assign_groups), assign_groups)
    total_tasks = 0
    for t in assign_groups:
        total_tasks += len(greedy_group[t - 1])

    print('assigned tasks:', total_tasks)

    return total_cost, assign_groups, greedy_assignment


def approximate_algorithm(group_greedy, detour_rate, TaskPoint, WorkerTrajectory, LocationTrajectoryPoint):
    Assignment = {}

    for j in WorkerTrajectory.keys():
    # for j in range(31, 32):

        # worker 轨迹上的所有轨迹对组合
        worker_traj_pair_combine = detour_pair(WorkerTrajectory[j]['Trajectory'])
        # 第 j 个 worker的绕路总距离
        tao = detour_rate * WorkerTrajectory[j]['distance']

        Assignment[j] = {}
        group_id = 0
        for group_list in group_greedy:
            group_id += 1

            flag = True

            if flag == False:
                continue

            else:
                # 当 group的规模超过一个 task
                if len(group_list) > 1:

                    # 采用近似算法生成group和轨迹点的最小生成树
                    spanning_tree, flag_tree = MST(group_list, WorkerTrajectory[j]['Trajectory'], TaskPoint, LocationTrajectoryPoint)
                    # 应用 DFS得到 group完成的闭环
                    task_schedule_circle = DFS(spanning_tree, flag_tree, group_list[0])

                    optimal_detour_cost = 99  # 不减轨迹段
                    optimal_start_task = -1
                    optimal_pair = []

                    # 遍历group的组合
                    for s in range(1, len(task_schedule_circle)):

                        start_task = task_schedule_circle[s]
                        end_task = task_schedule_circle[s - 1]

                        # group 内部完成所有任务的总距离
                        dist_2 = dist_task_schedule(task_schedule_circle, end_task, TaskPoint)

                        min_detour_cost = 99  # 不减去轨迹段的 cost

                        for pair in worker_traj_pair_combine:

                            # 偏移对到 group的两段距离
                            dist_1 = get_distance_hav(LocationTrajectoryPoint[pair[0]], TaskPoint[start_task + 1]['location'])
                            dist_3 = get_distance_hav(LocationTrajectoryPoint[pair[-1]], TaskPoint[end_task + 1]['location'])

                            if dist_1 + dist_2 + dist_3 <= tao:
                                available_cost = dist_1 + dist_2 + dist_3
                                available_pair = pair

                                if available_cost - worker_seg_traj_distance(j, WorkerTrajectory[j]['Trajectory'].index(
                                        available_pair[0]), WorkerTrajectory[j]['Trajectory'].index(available_pair[-1]), WorkerTrajectory, LocationTrajectoryPoint) < optimal_detour_cost: \
                                        #  and task_deadline_check(group_list, j, available_pair, worker_speed, TaskPoint, WorkerTrajectory, LocationTrajectoryPoint) == True:

                                    optimal_detour_cost = available_cost - worker_seg_traj_distance(j, WorkerTrajectory[j]['Trajectory'].index(
                                        available_pair[0]), WorkerTrajectory[j]['Trajectory'].index(available_pair[-1]), WorkerTrajectory, LocationTrajectoryPoint)
                                    optimal_start_task = start_task
                                    optimal_pair = available_pair

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

                        Assignment[j][group_id] = {}
                        Assignment[j][group_id]['schedule'] = right_list
                        Assignment[j][group_id]['detour'] = optimal_pair
                        Assignment[j][group_id]['cost'] = optimal_detour_cost

                # 当 group中只有一个任务
                if len(group_list) == 1:
                    task_center = group_list[0]

                    # 遍历轨迹点从左到右
                    near_dist = 99

                    # 找到离任务最近的轨迹点
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
                    optimal_cost = 99
                    optimal_detour_cost = 99
                    for l in range(0, len(candidate_traj)):
                        for r in range(len(candidate_traj) - 1, l, -1):
                            dist_5 = get_distance_hav(
                                LocationTrajectoryPoint[WorkerTrajectory[j]['Trajectory'][candidate_traj[l]]],
                                TaskPoint[task_center + 1]['location']) + get_distance_hav(
                                LocationTrajectoryPoint[WorkerTrajectory[j]['Trajectory'][candidate_traj[r]]],
                                TaskPoint[task_center + 1]['location'])
                            if dist_5 < tao:
                                
                                # print('pair:', l, r, dist_5, dist_5 - worker_seg_traj_distance(j, l, r))
                                best_detour_pair = [WorkerTrajectory[j]['Trajectory'][candidate_traj[l]],
                                                    WorkerTrajectory[j]['Trajectory'][candidate_traj[r]]]
                                optimal_cost = dist_5 - worker_seg_traj_distance(j, candidate_traj[l],
                                                                                candidate_traj[r], WorkerTrajectory, LocationTrajectoryPoint)
                                # optimal_detour_cost = dist_5
                                break
                        break

                    if len(candidate_traj) > 1 and len(best_detour_pair) > 0:
                        Assignment[j][group_id] = {}
                        Assignment[j][group_id]['schedule'] = [task_center]
                        Assignment[j][group_id]['detour'] = best_detour_pair
                        Assignment[j][group_id]['cost'] = optimal_cost
    
    return Assignment


def approximate_pruning_group(group_greedy, detour_rate, TaskPoint, WorkerTrajectory, LocationTrajectoryPoint):
    Assignment = {}

    for j in WorkerTrajectory.keys():
    # for j in range(31, 32):

        tao = detour_rate * WorkerTrajectory[j]['distance']
        # print('trajectory:', WorkerTrajectory[j]['Trajectory'], tao)
        Assignment[j] = {}
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

                spanning_tree, flag_tree = MST(group_list, WorkerTrajectory[j]['Trajectory'], TaskPoint, LocationTrajectoryPoint)
                task_schedule_circle = DFS(spanning_tree, flag_tree, group_list[0])

                # 当 group的规模超过一个 task
                if len(task_schedule_circle) > 1:

                    # 遍历group的组合种类
                    optimal_start_traj = -1
                    optimal_end_traj = -1
                    optimal_cost = 99  # 减去轨迹段
                    optimal_detour_cost = 99  # 不减轨迹段
                    optimal_start_task = -1

                    for s in range(1, len(task_schedule_circle)):

                        start_task = task_schedule_circle[s]
                        end_task = task_schedule_circle[s - 1]

                        # group 内部完成所有任务的总距离
                        dist_2 = dist_task_schedule(task_schedule_circle, end_task, TaskPoint)

                        min_start_traj = -1
                        min_end_traj = -1
                        min_cost_cost = 99  # 减去轨迹段的 cost
                        min_detour_cost = 99  # 不减去轨迹段的 cost

                        # 遍历轨迹点从左到右
                        for o in range(0, len(WorkerTrajectory[j]['Trajectory']) - 1):

                            min_cost = 99
                            # 偏出轨迹点离任务组的起始任务的距离
                            dist_1 = get_distance_hav(LocationTrajectoryPoint[WorkerTrajectory[j]['Trajectory'][o]],
                                                   TaskPoint[start_task + 1]['location'])

                            # 遍历轨迹点从右到左
                            start_traj = -1
                            end_traj = -1
                            for sss in range(len(WorkerTrajectory[j]['Trajectory'])-1, o, -1):
                                # 尾部任务离右侧遍历轨迹点的距离
                                dist_3 = get_distance_hav(
                                    LocationTrajectoryPoint[WorkerTrajectory[j]['Trajectory'][sss]],
                                    TaskPoint[end_task + 1]['location'])
                                # 一旦从最右侧起找到符合限制的偏入轨迹点，则停止，根据引理可以知道绕路最小
                                if dist_1 + dist_2 + dist_3 <= tao:

                                    min_cost = dist_1 + dist_2 + dist_3
                                    start_traj = o
                                    end_traj = sss
                                    break

                            if start_traj != -1 and min_cost - worker_seg_traj_distance(j, start_traj, end_traj, WorkerTrajectory, LocationTrajectoryPoint) < min_cost_cost: \
                            #  and task_deadline_check(group_list, j, [WorkerTrajectory[j]['Trajectory'][start_traj], WorkerTrajectory[j]['Trajectory'][end_traj]], worker_speed, TaskPoint, WorkerTrajectory, LocationTrajectoryPoint) == True:
                                
                                min_cost_cost = min_cost - worker_seg_traj_distance(j, start_traj, end_traj, WorkerTrajectory, LocationTrajectoryPoint)
                                min_start_traj = start_traj
                                min_end_traj = end_traj
                                min_detour_cost = min_cost

                        if min_cost_cost < optimal_cost:
                            optimal_detour_cost = min_detour_cost
                            optimal_start_traj = min_start_traj
                            optimal_end_traj = min_end_traj
                            optimal_start_task = start_task

                    if optimal_start_task != -1:

                        left_list = []
                        right_list = []
                        for i in range(0, len(task_schedule_circle)):
                            if task_schedule_circle.index(task_schedule_circle[i]) >= task_schedule_circle.index(optimal_start_task):
                                right_list.append(task_schedule_circle[i])
                            else:
                                left_list.append(task_schedule_circle[i])
                        right_list.extend(left_list)

                        Assignment[j][group_id] = {}
                        Assignment[j][group_id]['schedule'] = right_list
                        Assignment[j][group_id]['detour'] = [WorkerTrajectory[j]['Trajectory'][optimal_start_traj], WorkerTrajectory[j]['Trajectory'][optimal_end_traj]]
                        Assignment[j][group_id]['cost'] = optimal_detour_cost

                # 当 group中只有一个任务
                if len(task_schedule_circle) == 1:
                    task_center = task_schedule_circle[0]

                    # 遍历轨迹点从左到右
                    near_dist = 99

                    # 找到离任务最近的轨迹点
                    for oo in range(0, len(WorkerTrajectory[j]['Trajectory'])):

                        dist_4 = get_distance_hav(LocationTrajectoryPoint[WorkerTrajectory[j]['Trajectory'][oo]], TaskPoint[task_center + 1]['location'])
                        if dist_4 < near_dist:
                            near_dist = dist_4

                    # 根据绕路限制，筛选可达的轨迹点
                    candidate_traj = []
                    for o in range(0, len(WorkerTrajectory[j]['Trajectory'])):
                        if get_distance_hav(LocationTrajectoryPoint[WorkerTrajectory[j]['Trajectory'][o]], TaskPoint[task_center + 1]['location']) < tao - near_dist:
                            candidate_traj.append(o)

                    # 从左和右同时相对遍历，一旦有轨迹对符合绕路限制，停止遍历，返回轨迹对
                    best_detour_pair = []
                    optimal_cost = 99
                    optimal_detour_cost = 99
                    for l in range(0, len(candidate_traj)):
                        for r in range(len(candidate_traj)-1, l, -1):
                            dist_5 = get_distance_hav(
                                LocationTrajectoryPoint[WorkerTrajectory[j]['Trajectory'][candidate_traj[l]]],
                                TaskPoint[task_center + 1]['location']) + get_distance_hav(
                                LocationTrajectoryPoint[WorkerTrajectory[j]['Trajectory'][candidate_traj[r]]],
                                TaskPoint[task_center + 1]['location'])
                            if dist_5 < tao:

                                best_detour_pair = [WorkerTrajectory[j]['Trajectory'][candidate_traj[l]], WorkerTrajectory[j]['Trajectory'][candidate_traj[r]]]
                                optimal_cost = dist_5 - worker_seg_traj_distance(j, candidate_traj[l], candidate_traj[r], WorkerTrajectory, LocationTrajectoryPoint)
 
                                break
                        break

                    if len(candidate_traj) > 1 and len(best_detour_pair) > 0:
                        Assignment[j][group_id] = {}
                        Assignment[j][group_id]['schedule'] = [task_center]
                        Assignment[j][group_id]['detour'] = best_detour_pair
                        Assignment[j][group_id]['cost'] = optimal_cost

    return Assignment


def approximate_pruning_group_traj(group_greedy, detour_rate, TaskPoint, WorkerTrajectory, LocationTrajectoryPoint):

    Assignment = {}

    for j in WorkerTrajectory.keys():

        tao = detour_rate * WorkerTrajectory[j]['distance']
        # tao = 0.2

        Assignment[j] = {}

        group_id = 0

        for group_list in group_greedy:
            group_id += 1

            flag = True
            for i in group_list:
                if dist_task_traj(i, WorkerTrajectory[j]['Trajectory'], TaskPoint, LocationTrajectoryPoint)[-1] > 0.5 * tao:
                    flag = False
                    break

            if flag == False:
                continue

            else:

                if len(group_list) > 1:

                    spanning_tree, flag_tree = MST(group_list, WorkerTrajectory[j]['Trajectory'], TaskPoint, LocationTrajectoryPoint)
                    task_schedule_circle = DFS(spanning_tree, flag_tree, group_list[0])

                    optimal_cost = 99
                    optimal_start_task = -1
                    optimal_detour = []

                    near_dist = 99
                    n_e = -1
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

                        dist_2 = dist_task_schedule(task_schedule_circle, end_task, TaskPoint)

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
                                if dist_5 < tao:
                                    # and task_deadline_check(group_list, j, [WorkerTrajectory[j]['Trajectory'][candidate_traj[l]], WorkerTrajectory[j]['Trajectory'][candidate_traj[r]]], worker_speed, TaskPoint, WorkerTrajectory, LocationTrajectoryPoint) == True:
                                    
                                    best_detour_pair = [WorkerTrajectory[j]['Trajectory'][candidate_traj[l]],
                                                        WorkerTrajectory[j]['Trajectory'][candidate_traj[r]]]
                                    min_cost = dist_5 - worker_seg_traj_distance(j, candidate_traj[l], candidate_traj[r], WorkerTrajectory, LocationTrajectoryPoint)
                                    break
                            break

                        if min_cost < optimal_cost:
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

                        Assignment[j][group_id] = {}
                        Assignment[j][group_id]['schedule'] = right_list
                        Assignment[j][group_id]['detour'] = optimal_detour

                if len(group_list) == 1:
                    task_center = group_list[0]

                    near_dist = 99

                    n_e = -1
                    for oo in range(0, len(WorkerTrajectory[j]['Trajectory'])):

                        dist_4 = get_distance_hav(LocationTrajectoryPoint[WorkerTrajectory[j]['Trajectory'][oo]],
                                               TaskPoint[task_center + 1]['location'])
                        if dist_4 < near_dist:
                            near_dist = dist_4
                            n_e = oo

                    candidate_traj = []
                    for o in range(0, len(WorkerTrajectory[j]['Trajectory'])):
                        if get_distance_hav(LocationTrajectoryPoint[WorkerTrajectory[j]['Trajectory'][o]],
                                         TaskPoint[task_center + 1]['location']) < tao - near_dist:
                            candidate_traj.append(o)

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
                                break
                        break

                    if len(candidate_traj) > 1 and len(best_detour_pair) > 0:
                        Assignment[j][group_id] = {}
                        Assignment[j][group_id]['schedule'] = [task_center]
                        Assignment[j][group_id]['detour'] = best_detour_pair

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
    real_dis = 0
    for p in range(index_d, index_c):
        real_dis += get_distance_hav(LocationTrajectoryPoint[p], LocationTrajectoryPoint[p + 1])
    
    return real_task - real_dis


def greedy_assignment(Assignment_schedule):

    pair_list = []
    for wi in Assignment_schedule.keys():
        for gj in Assignment_schedule[wi].keys():

            if single_pair_cost(wi, gj, Assignment_schedule) >= 0:
                pair_list.append((wi, gj, len(Assignment_schedule[wi][gj]['schedule']), single_pair_cost(wi, gj, Assignment_schedule)))
            else:
                pair_list.append((wi, gj, len(Assignment_schedule[wi][gj]['schedule']), 0))
      