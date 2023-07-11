import os
import json
import re

def find_numbers(file_path):
    with open(file_path, 'r') as file:
        content = file.read()
        matches = re.findall(r'\b\d+(?:\.\d+)?(?:0+)?\b', content)
        if matches:
            first = matches[0]
            if '.' in first:
                first = float(first)
            else:
                first = int(first)
            
            if len(matches) > 2:
                second = matches[2]
                if '.' in second:
                    second = float(second)
                else:
                    second = int(second)
                return (first, second)
            else:
                return (first,)
    return None

os.system('psql -U postgres -f /home/setup.sql')
num_workers = 16
workers = f'set max_parallel_workers = {num_workers}; set max_parallel_workers_per_gather = {num_workers};' 
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
                json_dict[set][join][str(size_A)][str(size_B)]['build_index1_time_ms'] = list()
                json_dict[set][join][str(size_A)][str(size_B)]['build_index2_time_ms'] = list()
                json_dict[set][join][str(size_A)][str(size_B)]['join_time_ms'] = list()

for set in datasets:
    for size_A in sizes:
        s = 'uniform' if set=='Uniform' else 'OSM'
        pre_path = f'/home/data/postgis_{s}_'
        path_points = f'{pre_path}{size_A}_point_file_0.csv'
        path_poly_0 = f'{pre_path}{size_A}_polygon_file_0.csv'
        os.system(f'psql -U postgres -c "copy data_A from \'{path_points}\' delimiter \';\' csv header;"')
        os.system(f'psql -U postgres -c "copy data_C from \'{path_poly_0}\' delimiter \';\' csv header;"')
        for size_B in sizes:    
            path_poly = f'{pre_path}{size_B}_polygon_file_0.csv'
            path_poly_1 = f'{pre_path}{size_B}_polygon_file_1.csv'
            os.system(f'psql -U postgres -c "copy data_B from \'{path_poly}\' delimiter \';\' csv header;"')
            os.system(f'psql -U postgres -c "copy data_D from \'{path_poly_1}\' delimiter \';\' csv header;"')
            for i in range(3):
                #pip
                os.system(f'(psql -U postgres -c "{workers} create index A on data_A using gist(geom);") > log')
                tup = find_numbers('log')
                json_dict[set]['Point-in-Polygon'][str(size_A)][str(size_B)]['build_index1_time_ms'].append(tup[0])
                os.system(f'(psql -U postgres -c "{workers} create index B on data_B using gist(geom);") > log')
                tup = find_numbers('log')
                json_dict[set]['Point-in-Polygon'][str(size_A)][str(size_B)]['build_index2_time_ms'].append(tup[0])
                os.system(f'(psql -U postgres -c "{workers} select count(*) from data_A join data_B on ST_Intersects(data_A.geom,data_B.geom);") > log')
                tup = find_numbers('log')
                json_dict[set]['Point-in-Polygon'][str(size_A)][str(size_B)]['num_results'].append(tup[0])
                json_dict[set]['Point-in-Polygon'][str(size_A)][str(size_B)]['join_time_ms'].append(tup[1])
                os.system('psql -U postgres -c "drop index A, B;"')

                #pp
                os.system(f'(psql -U postgres -c "{workers} create index C on data_C using gist(geom);") > log')
                tup = find_numbers('log')
                json_dict[set]['Polygon-Polygon'][str(size_A)][str(size_B)]['build_index1_time_ms'].append(tup[0])
                os.system(f'(psql -U postgres -c "{workers} create index D on data_D using gist(geom);") > log')
                tup = find_numbers('log')
                json_dict[set]['Polygon-Polygon'][str(size_A)][str(size_B)]['build_index2_time_ms'].append(tup[0])
                os.system(f'(psql -U postgres -c "{workers} select count(*) from data_C join data_D on ST_Intersects(data_C.geom,data_D.geom);") > log')
                tup = find_numbers('log')
                json_dict[set]['Polygon-Polygon'][str(size_A)][str(size_B)]['num_results'].append(tup[0])
                json_dict[set]['Polygon-Polygon'][str(size_A)][str(size_B)]['join_time_ms'].append(tup[1])
                os.system('psql -U postgres -c "drop index C, D;"')

            os.system('psql -U postgres -c "delete from data_B;"')
            os.system('psql -U postgres -c "delete from data_D;"')
        os.system('psql -U postgres -c "delete from data_A;"')
        os.system('psql -U postgres -c "delete from data_C;"')


with open(f'runs.json', 'w') as file:
    json.dump(json_dict, file, indent = 2)

os.system('psql -U postgres -f /home/clear.sql')
