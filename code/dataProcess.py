from asyncio import Task
from collections import defaultdict
import math, os, random
import json
from math import sin, asin, cos, radians, fabs, sqrt
from secrets import choice

EARTH_RADIUS = 6371  # 地球平均半径，6371km

in_prefix = 'basic_datas/Berlin/'
out_prefix = 'data_result/'
synthetic_prefix = 'data_result/synthetic_data/'

# # Berlin
in_stoplocation_file = in_prefix + 'BusStopsBER.txt'
in_route_file = in_prefix + 'BusRoutesAsStopsBER.txt'
in_route_file_new = in_prefix + 'BusRoutesAsStopsBER_new.txt'
in_task_file = in_prefix + 'CoffeeShopsBER.txt'

out_stoplocation_file = out_prefix + 'stoplocation_berlin.json'
out_route_file = out_prefix + 'worker_berlin.json'
out_task_file = out_prefix + 'task_berlin.json'

in_edge_file = in_prefix + 'RoadEdgesBER.txt'
out_edge_file = in_prefix + 'RoadEdgesBER_new.txt'
in_vertice_file = in_prefix + 'RoadVerticesBER.txt'
synthetic_traj_file = synthetic_prefix + 'RouteBER_synthetic.txt'
synthetic_task_file = synthetic_prefix + 'TaskBER_synthetic.txt'

road_vertice_json = synthetic_prefix + 'RoadVerticesBER.json'
synthetic_task_json = synthetic_prefix + 'task_berlin_synthetic.json'
synthetic_worker_file = synthetic_prefix + 'worker_berlin_synthetic.json'

# Oslo
# in_stoplocation_file = in_prefix + 'BusStopsOSLO.txt'
# in_route_file = in_prefix + 'BusRoutesAsStopsOSLO.txt'
# in_route_file_new = in_prefix + 'BusRoutesAsStopsOLSO_new.txt'
# in_task_file = in_prefix + 'CoffeeShopsOSLO.txt'

# out_stoplocation_file = out_prefix + 'stoplocation_oslo.json'
# out_route_file = out_prefix + 'worker_oslo.json'
# out_task_file = out_prefix + 'task_oslo.json'

# in_edge_file = in_prefix + 'RoadEdgesOSLO.txt'
# out_edge_file = in_prefix + 'RoadEdgesOSLO_new.txt'

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

def stop_location(in_stoplocation_file):

    with open(in_stoplocation_file, 'r') as in_locationfile:
        LocationTrajectoryPoint = defaultdict(list)
        count = 0
        for line in in_locationfile.readlines():        
            line = line.strip('\n')
            row = line.split(' ')
            row = list(map(float, row))
            LocationTrajectoryPoint[count] = [row[1], row[2]]
            count += 1

    with open(out_stoplocation_file, 'w') as out_locationfile:
        json.dump(LocationTrajectoryPoint, out_locationfile)


def route_process(in_route_file):

    with open(in_route_file, 'r') as routefile, open(in_route_file_new, 'w') as new_route:
        count = 0
        for line in routefile:
            line = line.strip('\n')
            row = line.split(' ')
            row  = row[1:]

            stop_list = []
            if len(row) % 10 == 0:
                stop_list = [row[i:i + 10] for i in range(0, int(len(row) / 10) * 10, 10)]

            elif len(row) / 10 > 2:
                # n - 1 * 10 stops
                stop_list = [row[i:i + 10] for i in range(0, (int(len(row) / 10) - 1) * 10, 10)]
                stop_list_remain = row[(int(len(row) / 10) - 1) * 10:]
                stop_list.append(stop_list_remain[:len(stop_list_remain)//2])
                stop_list.append(stop_list_remain[len(stop_list_remain)//2:])
                
            else:
                stop_list.append(row[:len(row)//2])
                stop_list.append(row[len(row)//2:])
            
            for ls in stop_list:
                string  = ''
                for ele in ls:
                    string += '\t' + ele
                count += 1
                new_route.write(str(count) + string + '\n')

def worker_route(size):

    with open(out_stoplocation_file, 'r') as f_stop:
        stop_dict = json.load(f_stop)
    LocationTrajectoryPoint = defaultdict(list)
    for k in stop_dict.keys():
        LocationTrajectoryPoint[int(k)] = stop_dict[k]

    with open(in_route_file_new, 'r') as f:
        WorkerTrajectory = defaultdict(dict)
        counttask = 0
        for line in f.readlines():
            counttask += 1
            line = line.strip('\n')
            row = line.split('\t')
            row = list(map(int, row))

            if counttask == size + 1:
                break

            WorkerTrajectory[row[0]] = defaultdict(list)
            WorkerTrajectory[row[0]]['Trajectory'] = row[1:]

            dis = 0
            for j in range(0, len(WorkerTrajectory[row[0]]['Trajectory']) - 1):
                l1 = WorkerTrajectory[row[0]]['Trajectory'][j]
                l2 = WorkerTrajectory[row[0]]['Trajectory'][j + 1]
                dis += get_distance_hav(LocationTrajectoryPoint[l1], LocationTrajectoryPoint[l2])
            WorkerTrajectory[row[0]]['distance'] = dis

    with open(out_route_file, 'w') as f:
        json.dump(WorkerTrajectory, f)

def task_process(size, worker_speed, tw):
    with open(out_stoplocation_file, 'r') as f_stop:
        stop_dict = json.load(f_stop)
    LocationTrajectoryPoint = defaultdict(list)
    for k in stop_dict.keys():
        LocationTrajectoryPoint[int(k)] = stop_dict[k]

    with open(out_route_file, 'r') as f_route:
        route_dict = json.load(f_route)
    WorkerTrajectory = defaultdict(list)
    for k in route_dict.keys():
        WorkerTrajectory[int(k)] = route_dict[k]

    with open(in_task_file, 'r') as task_file:
        TaskPoint = defaultdict(dict)
        counttask = 0
        for line in task_file.readlines():
            counttask += 1
            if counttask == size + 1:
                break
            line = line.strip('\n')
            b = line.split(' ')
            b = list(map(float, b))
            k = counttask
            del b[0]
            del b[2]
            del b[2]
            TaskPoint[k] = {}
            TaskPoint[k]['location'] = b
            TaskPoint[k]['expiration'] = 0
    print('Location of task point:', len(TaskPoint))

    for t in TaskPoint.keys():
        min_dist = 100000
        for w in WorkerTrajectory.keys():
            if get_distance_hav(TaskPoint[t]['location'], LocationTrajectoryPoint[WorkerTrajectory[w]['Trajectory'][0]]) < min_dist:
                min_dist = get_distance_hav(TaskPoint[t]['location'], LocationTrajectoryPoint[WorkerTrajectory[w]['Trajectory'][0]])
        TaskPoint[t]['expiration'] = min_dist / worker_speed * tw

    with open(out_task_file, 'w') as task_dict:
        json.dump(TaskPoint, task_dict)


def interconnection_nodes():

    # road segment
    with open(in_edge_file, 'r') as edge_file, open(out_edge_file, 'w') as edge_new:
        road_dict = defaultdict(list)
        edge_list = [list(line.strip('\n').split(' '))[1:] for line in edge_file]
        # print('temp list:', temp_list)
        seg_list = [edge_list[0][0]]
        for edge in range(len(edge_list) - 1):
            if edge_list[edge][-1] == edge_list[edge + 1][0]:
                seg_list.append(edge_list[edge][-1])
            else:
                seg_list.append(edge_list[edge][-1])
                road_dict[len(road_dict)] = seg_list
                edge_new.write(str(len(road_dict)) + '\t' + str('\t'.join(seg_list)) + '\n')
                seg_list = [edge_list[edge + 1][0]]            
        seg_list.append(edge_list[edge + 1][-1])
        road_dict[len(road_dict)] = seg_list
        edge_new.write(str(len(road_dict)) + '\t' + str('\t'.join(seg_list)) + '\n')
        
    # interconnection edge
    interconnection_dict = defaultdict(list)
    for id in range(len(road_dict)):
        for id2 in range(id + 1, len(road_dict)):
            inter_list = list(set(road_dict[id]).intersection(set(road_dict[id2])))
            if len(inter_list) != 0:
                for inter in inter_list:
                    interconnection_dict[inter].append(id)
                    interconnection_dict[inter].append(id2)
                    interconnection_dict[inter] = list(set(interconnection_dict[inter]))
    
    with open('data_result/RoadInterconnectionBER.json', 'w') as inter_file:
        json.dump(interconnection_dict, inter_file)

def worker_synthetic(num, max_traj_size):

    with open(in_vertice_file, 'r') as vertic_file:
        vertice_dict = defaultdict(list)
        for line in vertic_file:
            row = line.strip('\n').split(' ')
            vertice_dict[int(row[0])] = list(map(float, row[1:]))
    
    with open(road_vertice_json, 'w') as f:
        json.dump(vertice_dict, f)
    
    road_dict = defaultdict(list)
    with open(out_edge_file, 'r') as seg_file:
        for line in seg_file:
            row = line.strip('\n').split('\t')
            road_dict[int(row[0])] = row[1:]

    with open('data_result/RoadInterconnectionBER.json', 'r') as inter_file:
        inter_dict = json.load(inter_file)
    interconnection_dict = defaultdict(list)
    for k in inter_dict.keys():
        interconnection_dict[int(k)] = inter_dict[k]
    
    # random select the start point as the random edge
    edge_list = []
    for _, seg in road_dict.items():
        for node in seg:
            edge_list.append(node)
    edge_list = list(set(edge_list))

    synthetic_trajs = []
    start_nodes = random.sample(edge_list, num)
    for node in start_nodes:
        sample_road = [node]
        # print('initial sample road:', sample_road)
        current_node = node
        for i in range(max_traj_size - 1):            
            # if start_node is the interconnection node
            if current_node in interconnection_dict.keys():
                # randomly select a road seg
                road_seg = random.sample(interconnection_dict[current_node], 1)
                node_index = road_dict[road_seg].index(current_node)
                if node_index == len(road_dict[road_seg]) - 1:
                    break
                else:
                    next_node = road_dict[road_seg][node_index + 1]
                    sample_road.append(next_node)
                    current_node = next_node          
            # if the start_node is not the normal node
            else:
                # find the corresponding seg id
                for seg_id, seg in road_dict.items():
                    if current_node in seg:
                        road_seg = seg_id
                        break
                node_index = road_dict[road_seg].index(current_node)
                if node_index == len(road_dict[road_seg]) - 1:
                    break
                else:
                    next_node = road_dict[road_seg][node_index + 1]
                    sample_road.append(next_node)
                    current_node = next_node
        synthetic_trajs.append(sample_road)

    valid_trajs = list(filter(lambda x: len(x) > 5, synthetic_trajs))
    print(len(valid_trajs))

    with open(synthetic_traj_file, 'w') as syn_file:
        count = 1
        for traj in list(valid_trajs):
            syn_file.write(str(count) + '\t' + str('\t'.join(traj)) + '\n')
            count += 1
    
    with open(synthetic_traj_file, 'r') as f:
        WorkerTrajectory = defaultdict(dict)
        for line in f.readlines():
            line = line.strip('\n')
            row = line.split('\t')
            row = list(map(int, row))

            WorkerTrajectory[row[0]] = defaultdict(list)
            WorkerTrajectory[row[0]]['Trajectory'] = row[1:]

            dis = 0
            for j in range(0, len(WorkerTrajectory[row[0]]['Trajectory']) - 1):
                l1 = WorkerTrajectory[row[0]]['Trajectory'][j]
                l2 = WorkerTrajectory[row[0]]['Trajectory'][j + 1]
                dis += get_distance_hav(vertice_dict[l1], vertice_dict[l2])
            WorkerTrajectory[row[0]]['distance'] = dis

    
    with open(synthetic_worker_file, 'w') as f:
        json.dump(WorkerTrajectory, f)

def task_synthetic(size_time):

    # check the distance of tasks in each group
    groups = []
    with open('data_result/task_group.txt', 'r') as group_file:
        for line in group_file:
            row = line.strip('\n').split('\t')
            row = list(map(int, row))
            groups.append(row)
    # print(groups)

    with open(out_task_file, 'r') as f_task:
        task_dict = json.load(f_task)
    TaskPoint = defaultdict(list)
    for k in task_dict.keys():
        TaskPoint[int(k)] = task_dict[k]

    
    for group in groups:
        dis_list = []
        lat_dis = []
        for task in range(len(group) - 1):
            for task2 in range(task + 1, len(group)):
                dis_list.append(get_distance_hav(TaskPoint[group[task] + 1]['location'], TaskPoint[group[task2] + 1]['location']))
                lat_diff = TaskPoint[group[task] + 1]['location'][0]-TaskPoint[group[task2] + 1]['location'][0]
                lon_diff = TaskPoint[group[task] + 1]['location'][1]-TaskPoint[group[task2] + 1]['location'][1]
                lat_dis.append([abs(lat_diff), abs(lon_diff)])
        # print('dis:', dis_list)
        # print('lat dis:', lat_dis)
    
    '''
     according to the meters to randomly form the tasks
     ensure the lat/lon dis is [0.000 - 0.0001]
     each task forming nearing 1-2 tasks
    '''
    with open(in_task_file, 'r') as real_task, open(synthetic_task_file, 'w') as synthetic_file:
        for line in real_task:
            row = line.strip('\n').split(' ')

            for i in range(size_time):
                synthetic_file.write(row[0] + '\t' + str(round(float(row[1])+random.uniform(0.00001, 0.0001), 6)) + '\t' + str(round( float(row[2]) + random.uniform(0.00001, 0.0001), 6)) + '\n' )

    with open(synthetic_task_file, 'r') as task_file:
        TaskPoint = defaultdict(dict)
        counttask = 0
        for line in task_file.readlines():
            counttask += 1
            # if synthetic_task_file == 'data_result/synthetic_data/TaskBER_synthetic.txt' and counttask == 5001:
            #     break
            # elif synthetic_task_file == 'data_result/synthetic_data/TaskOSLO_synthetic.txt' and counttask == 2001:
            #     break
            line = line.strip('\n')
            b = line.split('\t')
            b = list(map(float, b))
            k = counttask
            del b[0]
            TaskPoint[k] = {}
            TaskPoint[k]['location'] = b
            TaskPoint[k]['expiration'] = 0
    print('Location of task point:', len(TaskPoint), TaskPoint[1])

    with open(road_vertice_json, 'r') as f_stop:
        stop_dict = json.load(f_stop)
    LocationTrajectoryPoint = defaultdict(list)
    for k in stop_dict.keys():
        LocationTrajectoryPoint[int(k)] = stop_dict[k]

    with open(synthetic_worker_file, 'r') as f_route:
        route_dict = json.load(f_route)
    WorkerTrajectory = defaultdict(list)
    for k in route_dict.keys():
        WorkerTrajectory[int(k)] = route_dict[k]
    
    print(WorkerTrajectory[1])

    for t in TaskPoint.keys():
        max_dist = -1
        for w in WorkerTrajectory.keys():
            if get_distance_hav(TaskPoint[t]['location'], LocationTrajectoryPoint[WorkerTrajectory[w]['Trajectory'][0]]) > max_dist:
                min_dist = get_distance_hav(TaskPoint[t]['location'], LocationTrajectoryPoint[WorkerTrajectory[w]['Trajectory'][0]])
        TaskPoint[t]['expiration'] = min_dist / worker_speed * tw

    with open(synthetic_task_json, 'w') as task_dict:
        json.dump(TaskPoint, task_dict)






if __name__ == '__main__':

    worker_speed = 60
    tw = 4
    
    
    stop_location(in_stoplocation_file)

    route_process(in_route_file)

    worker_route(800)
    # task_process(2500, worker_speed, tw)

    # interconnection_nodes()
    # worker_synthetic(50, 10)
    # task_synthetic(3) # 生成 3*2550 tasks


    