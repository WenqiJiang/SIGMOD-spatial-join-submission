import cuspatial
import cudf
import cupy
import geopandas
import numpy as np
from shapely.geometry import *

host_dataframe = geopandas.read_file(geopandas.datasets.get_path("naturalearth_lowres"))
single_polygons = host_dataframe[host_dataframe['geometry'].type == "Polygon"]
gpu_dataframe = cuspatial.from_geopandas(single_polygons)
x_points = (cupy.random.random(10000000) - 0.5) * 360
y_points = (cupy.random.random(10000000) - 0.5) * 180
xy = cudf.DataFrame({"x": x_points, "y": y_points}).interleave_columns()
points = cuspatial.GeoSeries.from_points_xy(xy)


short_dataframe = gpu_dataframe.iloc[0:31]
geometry = short_dataframe['geometry']

points_in_polygon = cuspatial.point_in_polygon(
    points, geometry
)
sum_of_points_in_polygons_0_to_31 = points_in_polygon.sum()
print(sum_of_points_in_polygons_0_to_31.head())
