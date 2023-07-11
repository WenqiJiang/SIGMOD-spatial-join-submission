import os
import json
import re

def find_numbers(file_path):
    with open(file_path, 'r') as file:
        content = file.read()
        matches = re.findall(r'\b\d+(?:\.\d+)?(?:0+)?\b', content)
        if matches:
            try:
                part = int(matches[0])
                build = int(matches[2])
                num = int(matches[4])
                join = int(matches[5])
                return(part, build, num, join)
            except:
                return None
            
    return None

datasets = ['Uniform','OSM']
sizes = [int(1e5), int(1e6), int(1e7)]
partitions = [16, 64, 256]

json_dict = {}
for set in datasets:
    json_dict[set] = dict()
    for join in ['Point-in-Polygon', 'Polygon-Polygon']:
        json_dict[set][join] = dict()
        for size_A in sizes:
            json_dict[set][join][str(size_A)] = dict()
            for size_B in sizes:
                json_dict[set][join][str(size_A)][str(size_B)] = dict()
                for part in partitions:
                    json_dict[set][join][str(size_A)][str(size_B)][str(part)]['num_results'] = list()
                    json_dict[set][join][str(size_A)][str(size_B)][str(part)]['partition_time_ms'] = list()
                    json_dict[set][join][str(size_A)][str(size_B)][str(part)]['build_index_time_ms'] = list()
                    json_dict[set][join][str(size_A)][str(size_B)][str(part)]['join_time_ms'] = list()

for set in datasets:
    s = "uniform" if set == "Uniform" else "OSM"
    for size_A in sizes:
        #pip
        file_A_pip = f'/home/SpatialSpark/data/spatialspark_{s}_{size_A}_point_file_0.txt'
        #pp
        file_A_pp = f'/home/SpatialSpark/data/spatialspark_{s}_{size_A}_polygon_file_0.txt'
        for size_B in sizes:
            #pip
            file_B_pip = f'/home/SpatialSpark/data/spatialspark_{s}_{size_B}_polygon_file_0.txt'
            #pp
            file_B_pp = f'/home/SpatialSpark/data/spatialspark_{s}_{size_B}_polygon_file_1.txt'
            for part in partitions:
                #pip
                cmd = f'(/usr/bin/time /opt/spark-2.0.2/bin/spark-submit --driver-memory 150g --executor-memory 70g --conf spark.driver.maxResultSize=4g --conf spark.kryoserializer.buffer.max=2047m --master spark://d9336c0d81a5:7077 --class spatialspark.main.SpatialJoinApp /home/timing/target/scala-2.11/spatial-spark-assembly-1.1.1-beta-SNAPSHOT.jar --left {file_A_pip} --geom_left 0 --right {file_B_pip} --geom_right 0 --broadcast false --output {part} --predicate intersects --partition {part} --method stp --conf 32:32:0.1 --num_output 1) > log'
                for i in range(3):
                    try:
                        os.system(cmd)
                        os.system(f'rm -r {part}')
                        tup = find_numbers('log')
                        if(tup==None):
                            json_dict[set]['Point-in-Polygon'][str(size_A)][str(size_B)][str(part)]['num_results'].append('Error')
                            json_dict[set]['Point-in-Polygon'][str(size_A)][str(size_B)][str(part)]['partition_time_ms'].append('Error')
                            json_dict[set]['Point-in-Polygon'][str(size_A)][str(size_B)][str(part)]['build_index_time_ms'].append('Error')
                            json_dict[set]['Point-in-Polygon'][str(size_A)][str(size_B)][str(part)]['join_time_ms'].append('Error')
                        else:
                            json_dict[set]['Point-in-Polygon'][str(size_A)][str(size_B)][str(part)]['num_results'].append(tup[2])
                            json_dict[set]['Point-in-Polygon'][str(size_A)][str(size_B)][str(part)]['partition_time_ms'].append(tup[0])
                            json_dict[set]['Point-in-Polygon'][str(size_A)][str(size_B)][str(part)]['build_index_time_ms'].append(tup[1])
                            json_dict[set]['Point-in-Polygon'][str(size_A)][str(size_B)][str(part)]['join_time_ms'].append(tup[3])
                    except:
                        os.system(f'echo "Error at {part}" > log_{part}_error')

                #pp
                cmd = f'(/usr/bin/time /opt/spark-2.0.2/bin/spark-submit --driver-memory 150g --executor-memory 70g --conf spark.driver.maxResultSize=4g --conf spark.kryoserializer.buffer.max=2047m --master spark://d9336c0d81a5:7077 --class spatialspark.main.SpatialJoinApp /home/timing/target/scala-2.11/spatial-spark-assembly-1.1.1-beta-SNAPSHOT.jar --left {file_A_pp} --geom_left 0 --right {file_B_pp} --geom_right 0 --broadcast false --output {part} --predicate intersects --partition {part} --method stp --conf 32:32:0.1 --num_output 1) > log'
                for i in range(3):
                    try:
                        os.system(cmd)
                        os.system(f'rm -r {part}')
                        tup = find_numbers('log')
                        if(tup==None):
                            json_dict[set]['Polygon-Polygon'][str(size_A)][str(size_B)][str(part)]['num_results'].append('Error')
                            json_dict[set]['Polygon-Polygon'][str(size_A)][str(size_B)][str(part)]['partition_time_ms'].append('Error')
                            json_dict[set]['Polygon-Polygon'][str(size_A)][str(size_B)][str(part)]['build_index_time_ms'].append('Error')
                            json_dict[set]['Polygon-Polygon'][str(size_A)][str(size_B)][str(part)]['join_time_ms'].append('Error')
                        else:
                            json_dict[set]['Polygon-Polygon'][str(size_A)][str(size_B)][str(part)]['num_results'].append(tup[2])
                            json_dict[set]['Polygon-Polygon'][str(size_A)][str(size_B)][str(part)]['partition_time_ms'].append(tup[0])
                            json_dict[set]['Polygon-Polygon'][str(size_A)][str(size_B)][str(part)]['build_index_time_ms'].append(tup[1])
                            json_dict[set]['Polygon-Polygon'][str(size_A)][str(size_B)][str(part)]['join_time_ms'].append(tup[3])
                    except:
                        os.system(f'echo "Error at {part}" > log_{part}_error')


with open(f'results.json', 'w') as file:
    json.dump(json_dict, file, indent = 2)
