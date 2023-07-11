"""
Example Usage: 
    python pip_join.py --point_file_dir files/points.csv --polygon_file_dir files/polygons.csv --avg_leaf_size 1024 --batch_size 1000
"""

import cuspatial
import cudf
import cupy
import geopandas
import time
import argparse 
import os
import pandas as pd
import numpy as np

from shapely.geometry import *
from shapely import wkt


parser = argparse.ArgumentParser()
parser.add_argument('--point_file_dir', type=str, default='files/points.csv', help="input point file dir")
parser.add_argument('--polygon_file_dir', type=str, default='files/polygons.csv',  help="input polygon file dir")
parser.add_argument('--avg_leaf_size', type=int, default=16,  help="average number of items in a leaf node in the tree")
parser.add_argument('--batch_size', type=int, default=10000,  help="batch size of the polygons")
parser.add_argument('--measure_energy', type=int, default=0, help="While loop the join if measure energy is on")

args = parser.parse_args()
point_file_dir = args.point_file_dir
polygon_file_dir = args.polygon_file_dir
avg_leaf_size = args.avg_leaf_size
batch_size = args.batch_size
measure_energy = args.measure_energy

if batch_size > 1000:
    print("Warning: batch size of 10K resulted in OOM on 10M point dataset on 3090")
    print("Warning: batch size of 100K resulted in OOM on 1M point dataset on 3090")
    print("current bath size: ", batch_size)

def load_geodf(path):
    """
    Load a csv file first as pandas dataframe, then convert it to geopandas df.
    Note: the input file must has the geometry column
    """
    dtype = {"geometry": str}
    df = pd.read_csv(path, dtype=dtype)
    df["geometry"] = geopandas.GeoSeries.from_wkt(df["geometry"])
    gdf = geopandas.GeoDataFrame(df, geometry="geometry")
    
    return gdf

def get_point_xy(point):
    """
    Input: 
        type: shapely.geometry.polygon.Point, e.g., point_gdf['geometry'][0]
    Output:
        x, y
    """
    x = point.xy[0][0]
    y = point.xy[1][0]
    return x, y

def get_point_boundaries(point_column):
    """
    Input: 
        a list (column) of shapely.geometry.polygon.Point, e.g., point_gdf['geometry']
    Output:
        x_min, x_max, y_min, y_max
    """
    x_list_all, y_list_all = [], []
    for point in point_column:
        x, y = get_point_xy(point)
        x_list_all.append(x)
        y_list_all.append(y)
    x_min = np.amin(x_list_all)
    x_max = np.amax(x_list_all)
    y_min = np.amin(y_list_all)
    y_max = np.amax(y_list_all)
    return x_min, x_max, y_min, y_max

def get_polygon_xy_list(polygon):
    """
    Input: 
        type: shapely.geometry.polygon.Polygon, e.g., polygon_gdf['geometry'][0]
    Output:
        x_list, y_list. Each is a list of coordinates on x and y.
    """
    x_list = []
    y_list = []
    for e in polygon.exterior.coords.xy[0]:
        x_list.append(e)
    for e in polygon.exterior.coords.xy[1]:
        y_list.append(e)
    return x_list, y_list

def get_polygon_boundaries(polygon_column):
    """
    Input: 
        a list (column) of shapely.geometry.polygon.Polygon, e.g., polygon_gdf['geometry']
    Output:
        x_min, x_max, y_min, y_max
    """
    x_list_all, y_list_all = [], []
    for polygon in polygon_column:
        x_list, y_list = get_polygon_xy_list(polygon)
        x_list_all += x_list
        y_list_all += y_list
    x_min = np.amin(x_list_all)
    x_max = np.amax(x_list_all)
    y_min = np.amin(y_list_all)
    y_max = np.amax(y_list_all)
    return x_min, x_max, y_min, y_max

def get_tree_parameters(n_points, avg_leaf_size, x_min, x_max, y_min, y_max):
    """
    Given the number of points to index, and an average leaf_size, return the tree settings:
        scale (some normalization parameters), max_depth (tree depth), max_size (inner node size)
    """
    max_depth = 2
    while True:
        if avg_leaf_size * (4 ** max_depth) >= n_points:
            break
        else:
            max_depth += 1
    
    max_size = int(np.ceil(avg_leaf_size / 4))
    scale = int(max(x_max - x_min, y_max - y_min) / ((1 << max_depth) + 2)) * 10
    # scale = int(max(x_max - x_min, y_max - y_min) / ((1 << max_depth) + 2)) + 1
    print('Set the scale to the default x10, otherwise there are cases where results are 0 on large datasets')
    
    return scale, max_depth, max_size
    
if __name__ == '__main__':
    
    # Load data
    print("Loading data...")
    point_gdf = load_geodf(point_file_dir)
    polygon_gdf = load_geodf(polygon_file_dir)
        
    # data to GPU
    print("Data to GPU...")
    batch_num = int(np.ceil(polygon_gdf['geometry'].count() / batch_size))
    start_cp = time.time()
    gpu_point_df = cuspatial.from_geopandas(point_gdf)
    points_gpu = gpu_point_df['geometry']
    gpu_polygon_df = cuspatial.from_geopandas(polygon_gdf)
    polygons_gpu = gpu_polygon_df['geometry']
    end_cp = time.time()
    time_cp = end_cp - start_cp
    print('time data structure convert and data movement CPU -> GPU: {} ms'.format(1000 * time_cp))

    print(gpu_point_df.head())
    print(gpu_polygon_df.head())
        
    # unindexed join
    if polygons_gpu.count() <= 31:
        print("Unindexed join...")
        start_unindexed_join = time.time()
        points_in_polygon = cuspatial.point_in_polygon(points_gpu, polygons_gpu)
        sum_of_points_in_polygons = points_in_polygon.sum()
        end_unindexed_join = time.time()
        time_unindexed_join = end_unindexed_join - start_unindexed_join
        print('total pairs:', sum_of_points_in_polygons.sum())
        print('time unindexed join: {} ms'.format(1000 * time_unindexed_join))
    else:
        print("skip unindexed join due to the large polygon number (>= 32), which will run into errors")
        # print("RuntimeError: cuSpatial failure at: /opt/conda/conda-bld/work/cpp/include/cuspatial/experimental/detail/point_in_polygon.cuh:130: Number of polygons cannot exceed 31")

    # indexed join
    print("Indexed Join...")

    # construct index
    point_x_min, point_x_max, point_y_min, point_y_max = get_point_boundaries(point_gdf['geometry'])
    polygon_x_min, polygon_x_max, polygon_y_min, polygon_y_max = get_polygon_boundaries(polygon_gdf['geometry'])
    x_min = np.amin([point_x_min, polygon_x_min])
    x_max = np.amax([point_x_max, polygon_x_max])
    y_min = np.amin([point_y_min, polygon_y_min])
    y_max = np.amax([point_y_max, polygon_y_max])
    n_points = point_gdf['geometry'].count()
    scale, max_depth, max_size = get_tree_parameters(n_points, avg_leaf_size, x_min, x_max, y_min, y_max)
    print(f'tree parameters: scale:{scale}, max_depth:{max_depth}, max_size:{max_size}')
    print("Warning: if the join results are not precise, it could due to insufficient scale parameter")

    start_idx = time.time()
    point_indices, quadtree = cuspatial.quadtree_on_points(points_gpu,
                                                    x_min, 
                                                    x_max, 
                                                    y_min, 
                                                    y_max,
                                                    scale,
                                                    max_depth,
                                                    max_size)
    end_idx = time.time()
    time_idx = end_idx - start_idx
    
    # batch processing of polygon dataset
    while True:
        start_join = time.time()
        join_count = 0
        for batch_id in range(batch_num):

            print('batch: ', batch_id)
            start_id = batch_id * batch_size
            end_id = (batch_id + 1) * batch_size
            # if end_id >= polygon_gdf['geometry'].count():
            #     end_id = polygon_gdf['geometry'].count() - 1

            # print(polygon_gdf['geometry'][start_id: end_id])
            
            polygon_batch = polygons_gpu.iloc[start_id: end_id]
            # print(polygon_batch)
            # polygon intersect index
            poly_bboxes = cuspatial.polygon_bounding_boxes(polygon_batch)
            # print(poly_bboxes)
            intersections = cuspatial.join_quadtree_and_bounding_boxes(
                quadtree,
                poly_bboxes,
                x_min, 
                x_max, 
                y_min, 
                y_max,
                scale,
                max_depth
            )
            # print(intersections)
            polygons_and_points = cuspatial.quadtree_point_in_polygon(
                intersections,
                quadtree,
                point_indices,
                points_gpu,
                polygon_batch
            )
            # print(polygons_and_points)
            # print(polygons_and_points['polygon_index'].count())
            join_count += polygons_and_points['polygon_index'].count()
        
        end_join = time.time()
        time_join = end_join - start_join

        print('total join pairs:', join_count)
        print('time index: {} ms'.format(1000 * time_idx))
        print('time join: {} ms'.format(1000 * time_join))
        
        if not measure_energy:
            break