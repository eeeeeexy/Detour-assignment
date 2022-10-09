import matplotlib.pyplot as plt
import numpy as np


# task coalition range
num_x = [0.5, 0.6, 0.7, 0.8, 0.9]

num_exact = [1737, 1737, 1740, 1723, 1699]
num_approximate = [1706, 1699, 1661, 1642, 1602]
num_app_group = [1697, 1693, 1664, 1651, 1612]
num_app_group_traj = [1674, 1669, 1637, 1633, 1591]
num_pnn = [1390, 1390, 1390, 1390, 1390]

unit_exact = [0, 138.3181198, 131.7304447, 115.2113843, 110.8959248, 110.6721702]
unit_approximate = [161.0124894, 147.5239305, 148.6160692, 142.2369709, 147.2249899, 162.627548]
unit_app_group = [167.3271875, 176.5581913, 169.2601618, 165.217678, 166.9950502, 182.5695695]
unit_app_group_traj = [166.8500517, 142.933022, 152.3959944, 158.7831076, 160.5761647, 172.4153128]
unit_pnn = [655.4160124, 655.4160124, 655.4160124, 655.4160124, 655.4160124, 655.4160124]

time_exact = [0, 2651.264999, 3060.29208, 2938.796565, 5053.481997, 5193.030318]
time_approximate = [268.6402996, 250.371768, 261.6560591, 243.8818611, 247.4995543, 250.0633194]
time_app_group = [15.43586478, 13.57312894, 13.63576756, 12.1286684, 11.70507193, 11.3372581]
time_app_group_traj = [13.34077311, 11.80542507, 11.95969491, 10.73680382, 10.44183974, 9.983532524]
time_app_group_traj_parallel = [2.409038591, 2.143703222, 2.250228214, 2.030991411, 2.115844488, 1.882180452]
time_pnn = [89.01304021, 89.01304021, 89.01304021, 89.01304021, 89.01304021, 89.01304021]


# num image
plt.plot(num_x, num_exact, 'k*-', alpha=1, linewidth=1, label='acc')
plt.plot(num_x, num_approximate, 'ks-', alpha=1, linewidth=1, label='acc')
plt.plot(num_x, num_app_group, 'ko-', alpha=1, linewidth=1, label='acc')
plt.plot(num_x, num_app_group_traj, 'k*-', alpha=1, linewidth=1, label='acc')
plt.plot(num_x, num_pnn, 'k*--', alpha=1, linewidth=1, label='acc')

plt.legend()