import os
import json
import re

def find_numbers(file_path):
    with open(file_path, 'r') as file:
        content = file.read()
        matches = re.findall(r'\b\d+(?:\.\d+)?(?:0+)?\b', content)
        if matches:
            try:
                part1 = float(matches[0])*1000
                part2 = float(matches[1])*1000
                build = float(matches[2])*1000
                num = int(matches[3])
                join = float(matches[4])*1000
                return(part1, part2, build, num, join)
            except:
                return None
            
    return None

datasets = ['Uniform','OSM']
sizes = [int(1e5), int(1e6), int(1e7)]

json_dict = {}
for set in datasets:
    json_dict[set] = dict()
    for join in ['Point-in-Polygon', 'Polygon-Polygon']:
        json_dict[set][join] = dict()
        for size_A in sizes:
            json_dict[set][join][str(size_A)] = dict()
            for size_B in sizes:
                json_dict[set][join][str(size_A)][str(size_B)] = dict()

                json_dict[set][join][str(size_A)][str(size_B)]['num_results'] = list()
                json_dict[set][join][str(size_A)][str(size_B)]['partition1_time_ms'] = list()
                json_dict[set][join][str(size_A)][str(size_B)]['partition2_time_ms'] = list()
                json_dict[set][join][str(size_A)][str(size_B)]['build_index_time_ms'] = list()
                json_dict[set][join][str(size_A)][str(size_B)]['join_time_ms'] = list()

for set in datasets:
    for size_A in sizes:
        for size_B in sizes:
            for i in range(3):
                if set == "Uniform":
                    #pip
                    file_A = f'postgis_uniform_{size_A}_point_file_0'
                    file_B = f'postgis_uniform_{size_B}_polygon_file_0_set_0'
                    os.system(f'(/usr/local/spark/bin/spark-submit run.py --file1 {file_A} --file2 {file_B}) > log')
                    tup = find_numbers('log')
                    if(tup==None):
                        json_dict[set]['Point-in-Polygon'][str(size_A)][str(size_B)]['num_results'].append('Error')
                        json_dict[set]['Point-in-Polygon'][str(size_A)][str(size_B)]['partition1_time_ms'].append('Error')
                        json_dict[set]['Point-in-Polygon'][str(size_A)][str(size_B)]['partition2_time_ms'].append('Error')
                        json_dict[set]['Point-in-Polygon'][str(size_A)][str(size_B)]['build_index_time_ms'].append('Error')
                        json_dict[set]['Point-in-Polygon'][str(size_A)][str(size_B)]['join_time_ms'].append('Error')
                    else:
                        json_dict[set]['Point-in-Polygon'][str(size_A)][str(size_B)]['num_results'].append(tup[3])
                        json_dict[set]['Point-in-Polygon'][str(size_A)][str(size_B)]['partition1_time_ms'].append(tup[0])
                        json_dict[set]['Point-in-Polygon'][str(size_A)][str(size_B)]['partition2_time_ms'].append(tup[1])
                        json_dict[set]['Point-in-Polygon'][str(size_A)][str(size_B)]['build_index_time_ms'].append(tup[2])
                        json_dict[set]['Point-in-Polygon'][str(size_A)][str(size_B)]['join_time_ms'].append(tup[4])
                    #pp
                    file_A = f'postgis_uniform_{size_A}_polygon_file_0_set_0'
                    file_B = f'postgis_uniform_{size_B}_polygon_file_1_set_0'
                    os.system(f'(/usr/local/spark/bin/spark-submit run.py --file1 {file_A} --file2 {file_B}) > log')
                    tup = find_numbers('log')
                    if(tup==None):
                        json_dict[set]['Polygon-Polygon'][str(size_A)][str(size_B)]['num_results'].append('Error')
                        json_dict[set]['Polygon-Polygon'][str(size_A)][str(size_B)]['partition1_time_ms'].append('Error')
                        json_dict[set]['Polygon-Polygon'][str(size_A)][str(size_B)]['partition2_time_ms'].append('Error')
                        json_dict[set]['Polygon-Polygon'][str(size_A)][str(size_B)]['build_index_time_ms'].append('Error')
                        json_dict[set]['Polygon-Polygon'][str(size_A)][str(size_B)]['join_time_ms'].append('Error')
                    else:
                        json_dict[set]['Polygon-Polygon'][str(size_A)][str(size_B)]['num_results'].append(tup[3])
                        json_dict[set]['Polygon-Polygon'][str(size_A)][str(size_B)]['partition1_time_ms'].append(tup[0])
                        json_dict[set]['Polygon-Polygon'][str(size_A)][str(size_B)]['partition2_time_ms'].append(tup[1])
                        json_dict[set]['Polygon-Polygon'][str(size_A)][str(size_B)]['build_index_time_ms'].append(tup[2])
                        json_dict[set]['Polygon-Polygon'][str(size_A)][str(size_B)]['join_time_ms'].append(tup[4])
                elif set == 'OSM':
                    #pip
                    file_A = f'postgis_OSM_{size_A}_point_file_0'
                    file_B = f'postgis_OSM_{size_B}_polygon_file_0'
                    os.system(f'(/usr/local/spark/bin/spark-submit run.py --file1 {file_A} --file2 {file_B}) > log')
                    tup = find_numbers('log')
                    if(tup==None):
                        json_dict[set]['Point-in-Polygon'][str(size_A)][str(size_B)]['num_results'].append('Error')
                        json_dict[set]['Point-in-Polygon'][str(size_A)][str(size_B)]['partition1_time_ms'].append('Error')
                        json_dict[set]['Point-in-Polygon'][str(size_A)][str(size_B)]['partition2_time_ms'].append('Error')
                        json_dict[set]['Point-in-Polygon'][str(size_A)][str(size_B)]['build_index_time_ms'].append('Error')
                        json_dict[set]['Point-in-Polygon'][str(size_A)][str(size_B)]['join_time_ms'].append('Error')
                    else:
                        json_dict[set]['Point-in-Polygon'][str(size_A)][str(size_B)]['num_results'].append(tup[3])
                        json_dict[set]['Point-in-Polygon'][str(size_A)][str(size_B)]['partition1_time_ms'].append(tup[0])
                        json_dict[set]['Point-in-Polygon'][str(size_A)][str(size_B)]['partition2_time_ms'].append(tup[1])
                        json_dict[set]['Point-in-Polygon'][str(size_A)][str(size_B)]['build_index_time_ms'].append(tup[2])
                        json_dict[set]['Point-in-Polygon'][str(size_A)][str(size_B)]['join_time_ms'].append(tup[4])
                    #pp
                    file_A = f'postgis_OSM_{size_A}_polygon_file_0'
                    file_B = f'postgis_OSM_{size_B}_polygon_file_1'
                    os.system(f'(/usr/local/spark/bin/spark-submit run.py --file1 {file_A} --file2 {file_B}) > log')
                    tup = find_numbers('log')
                    if(tup==None):
                        json_dict[set]['Polygon-Polygon'][str(size_A)][str(size_B)]['num_results'].append('Error')
                        json_dict[set]['Polygon-Polygon'][str(size_A)][str(size_B)]['partition1_time_ms'].append('Error')
                        json_dict[set]['Polygon-Polygon'][str(size_A)][str(size_B)]['partition2_time_ms'].append('Error')
                        json_dict[set]['Polygon-Polygon'][str(size_A)][str(size_B)]['build_index_time_ms'].append('Error')
                        json_dict[set]['Polygon-Polygon'][str(size_A)][str(size_B)]['join_time_ms'].append('Error')
                    else:
                        json_dict[set]['Polygon-Polygon'][str(size_A)][str(size_B)]['num_results'].append(tup[3])
                        json_dict[set]['Polygon-Polygon'][str(size_A)][str(size_B)]['partition1_time_ms'].append(tup[0])
                        json_dict[set]['Polygon-Polygon'][str(size_A)][str(size_B)]['partition2_time_ms'].append(tup[1])
                        json_dict[set]['Polygon-Polygon'][str(size_A)][str(size_B)]['build_index_time_ms'].append(tup[2])
                        json_dict[set]['Polygon-Polygon'][str(size_A)][str(size_B)]['join_time_ms'].append(tup[4])

with open(f'runs.json', 'w') as file:
    json.dump(json_dict, file, indent = 2)
