# FPGA Performance test scripts

Run a single test (two datasets): perf_test.py

Run all experiments (various datasets, max node entry size, etc.): run_all_experiments.py

## Run all experiments using 16 PEs

Run on all dataset combinations.

```
python run_all_experiments.py \
--out_json_fname FPGA_perf_16_PE_3_runs.json \
--overwrite 0 \
--FPGA_project_dir /mnt/scratch/wenqi/spatial-join-on-FPGA/FPGA_BFS/BFS_multi_PE_v3.2_16_PE \
--cpp_exe_dir /mnt/scratch/wenqi/spatial-join-baseline/cpp/FPGA_index_constructor \
--get_tree_depth_py_dir /mnt/scratch/wenqi/spatial-join-baseline/python/get_tree_depth.py \
--C_file_dir /mnt/scratch/wenqi/spatial-join-baseline/generated_data \
--num_runs 3 > log_run_all_experiments
```

## Run FPGA PE scalability test

Test the performance scalability with different PE numbers, under different page sizes. All of them are executed on 10M datasets

```
python run_all_experiments.py \
--out_json_fname FPGA_perf_1_PE_3_runs.json \
--overwrite 0 \
--FPGA_project_dir /mnt/scratch/wenqi/spatial-join-on-FPGA/FPGA_BFS/BFS_multi_PE_v3.2_1_PE \
--cpp_exe_dir /mnt/scratch/wenqi/spatial-join-baseline/cpp/FPGA_index_constructor \
--get_tree_depth_py_dir /mnt/scratch/wenqi/spatial-join-baseline/python/get_tree_depth.py \
--C_file_dir /mnt/scratch/wenqi/spatial-join-baseline/generated_data \
--dataset_size 10000000 \
--num_runs 3 > log_run_all_experiments

python run_all_experiments.py \
--out_json_fname FPGA_perf_2_PE_3_runs.json \
--overwrite 0 \
--FPGA_project_dir /mnt/scratch/wenqi/spatial-join-on-FPGA/FPGA_BFS/BFS_multi_PE_v3.2_2_PE \
--cpp_exe_dir /mnt/scratch/wenqi/spatial-join-baseline/cpp/FPGA_index_constructor \
--get_tree_depth_py_dir /mnt/scratch/wenqi/spatial-join-baseline/python/get_tree_depth.py \
--C_file_dir /mnt/scratch/wenqi/spatial-join-baseline/generated_data \
--dataset_size 10000000 \
--num_runs 3 > log_run_all_experiments

python run_all_experiments.py \
--out_json_fname FPGA_perf_4_PE_3_runs.json \
--overwrite 0 \
--FPGA_project_dir /mnt/scratch/wenqi/spatial-join-on-FPGA/FPGA_BFS/BFS_multi_PE_v3.2_4_PE \
--cpp_exe_dir /mnt/scratch/wenqi/spatial-join-baseline/cpp/FPGA_index_constructor \
--get_tree_depth_py_dir /mnt/scratch/wenqi/spatial-join-baseline/python/get_tree_depth.py \
--C_file_dir /mnt/scratch/wenqi/spatial-join-baseline/generated_data \
--dataset_size 10000000 \
--num_runs 3 > log_run_all_experiments

python run_all_experiments.py \
--out_json_fname FPGA_perf_8_PE_3_runs.json \
--overwrite 0 \
--FPGA_project_dir /mnt/scratch/wenqi/spatial-join-on-FPGA/FPGA_BFS/BFS_multi_PE_v3.2_8_PE \
--cpp_exe_dir /mnt/scratch/wenqi/spatial-join-baseline/cpp/FPGA_index_constructor \
--get_tree_depth_py_dir /mnt/scratch/wenqi/spatial-join-baseline/python/get_tree_depth.py \
--C_file_dir /mnt/scratch/wenqi/spatial-join-baseline/generated_data \
--dataset_size 10000000 \
--num_runs 3 > log_run_all_experiments
```
