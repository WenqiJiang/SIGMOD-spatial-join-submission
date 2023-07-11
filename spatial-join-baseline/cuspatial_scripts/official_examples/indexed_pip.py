import cuspatial
import cudf
import cupy
import geopandas
import numpy as np
from shapely.geometry import *

# polygon dataset
host_dataframe = geopandas.read_file(geopandas.datasets.get_path("naturalearth_lowres"))
single_polygons = host_dataframe[host_dataframe['geometry'].type == "Polygon"]
print("single polygons:\n", single_polygons)
gpu_dataframe = cuspatial.from_geopandas(single_polygons)
polygons = gpu_dataframe['geometry']

# point dataset
x_points = (cupy.random.random(10000000) - 0.5) * 360
y_points = (cupy.random.random(10000000) - 0.5) * 180
print("x points: ", x_points)
xy = cudf.DataFrame({"x": x_points, "y": y_points}).interleave_columns()
points = cuspatial.GeoSeries.from_points_xy(xy)

# quadtree on points
scale = 5
max_depth = 7
max_size = 125
point_indices, quadtree = cuspatial.quadtree_on_points(points,
                                                       x_points.min(),
                                                       x_points.max(),
                                                       y_points.min(),
                                                       y_points.max(),
                                                       scale,
                                                       max_depth,
                                                       max_size)
print(point_indices.head())
print(quadtree.head())

# Indexed Spatial Joins 

poly_bboxes = cuspatial.polygon_bounding_boxes(
    polygons
)
# Intersects the polygon bounding box and the quad tree
intersections = cuspatial.join_quadtree_and_bounding_boxes(
    quadtree,
    poly_bboxes,
    polygons.polygons.x.min(),
    polygons.polygons.x.max(),
    polygons.polygons.y.min(),
    polygons.polygons.y.max(),
    scale,
    max_depth
)
print(intersections.head())
# Only the points are indexed, not the polygons
polygons_and_points = cuspatial.quadtree_point_in_polygon(
    intersections,
    quadtree,
    point_indices,
    points,
    polygons
)
print(polygons_and_points.head())
