import argparse
import time
from sedona.core.geom.envelope import Envelope
from sedona.core.spatialOperator import RangeQuery
from pyspark.sql import SparkSession
from sedona.core.enums import GridType
from sedona.core.enums import IndexType
from sedona.core.spatialOperator import JoinQuery
from sedona.utils.adapter import Adapter
from sedona.register import SedonaRegistrator  
from sedona.utils import SedonaKryoRegistrator, KryoSerializer
spark = SparkSession.builder.master("local[*]").appName("Sedona App").config("spark.serializer", KryoSerializer.getName).config("spark.kryo.registrator", SedonaKryoRegistrator.getName).getOrCreate()
SedonaRegistrator.registerAll(spark)

parser = argparse.ArgumentParser(description='Sedona script')
parser.add_argument('--file1', type=str, help='Path to file 1')
parser.add_argument('--file2', type=str, help='Path to file 2')
args = parser.parse_args()
file1 = args.file1
file2 = args.file2

build_on_spatial_partitioned_rdd = True
using_index = True

# Load dataset 1
data0 = spark.read.option("delimiter",";").option("header","true").csv(f"data/{file1}.csv")
data0.createOrReplaceTempView("test0")
test0_geom = spark.sql(
                         """SELECT gid,
                            st_geomFromWKT(geom) as mbr
                            from test0""" 
                      )

spatial_rdd = Adapter.toSpatialRdd(test0_geom, "mbr")
spatial_rdd.analyze()
start = time.time()
spatial_rdd.spatialPartitioning(GridType.KDBTREE)
print(f'Partitioning one: {time.time() - start}')

# Load dataset 2
data1 = spark.read.option("delimiter",";").option("header","true").csv(f"data/{file2}.csv")
data1.createOrReplaceTempView("test1")
test1_geom = spark.sql(
                         """SELECT gid,
                            st_geomFromWKT(geom) as mbr
                            from test1""" 
                      )
spatial_rdd_1 = Adapter.toSpatialRdd(test1_geom, "mbr")
spatial_rdd_1.analyze()
start = time.time()
spatial_rdd_1.spatialPartitioning(spatial_rdd.getPartitioner())
print(f'Partitioning two: {time.time() - start}')

if data0.count() > data1.count():
    # index on spatial_rdd
    start = time.time()
    spatial_rdd.buildIndex(IndexType.RTREE, build_on_spatial_partitioned_rdd)
    spatial_rdd.indexedRDD.count()
    print(f"build_index: {time.time()-start}")
else:
    #index on spatial_rdd_1
    start = time.time()
    spatial_rdd_1.buildIndex(IndexType.RTREE, build_on_spatial_partitioned_rdd)
    spatial_rdd_1.indexedRDD.count()
    print(f"build_index: {time.time()-start}")



# Perform spatial join

result = JoinQuery.SpatialJoinQueryFlat(spatial_rdd, spatial_rdd_1, using_index, True)
start = time.time()
# Count triggers the evaluation and returns the number of results
print("Num results: ", result.count())
print(f'Join time: {time.time() - start}')

