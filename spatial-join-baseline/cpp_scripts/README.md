# CPP test

## Run all multi-thread experiments

Either run the following one script (for around an hour):

```
python cpp_run_all_multithread_experiments.py \
--out_json_fname CPU_perf_16_threads.json \
--overwrite 0 \
--cpp_exe_dir /mnt/scratch/wenqi/spatial-join-baseline/cpp/multithread \
--C_file_dir /mnt/scratch/wenqi/spatial-join-baseline/generated_data \
--num_threads 16 \
--parallel_approach bfs_dynamic \
--num_runs 3 \
--log_name log_mt
```

Or running several scripts on different machines and manually merge the json:

```
python cpp_run_all_multithread_experiments.py \
--out_json_fname CPU_perf_16_threads_OSM_PIP.json \
--overwrite 0 \
--cpp_exe_dir /mnt/scratch/wenqi/spatial-join-baseline/cpp/multithread \
--C_file_dir /mnt/scratch/wenqi/spatial-join-baseline/generated_data \
--num_threads 16 \
--parallel_approach bfs_dynamic \
--num_runs 3 \
--dataset OSM \
--join_type Point-in-Polygon \
--log_name log_OSM_PIP

python cpp_run_all_multithread_experiments.py \
--out_json_fname CPU_perf_16_threads_OSM_PP.json \
--overwrite 0 \
--cpp_exe_dir /mnt/scratch/wenqi/spatial-join-baseline/cpp/multithread \
--C_file_dir /mnt/scratch/wenqi/spatial-join-baseline/generated_data \
--num_threads 16 \
--parallel_approach bfs_dynamic \
--num_runs 3 \
--dataset OSM \
--join_type Polygon-Polygon \
--log_name log_OSM_PP


python cpp_run_all_multithread_experiments.py \
--out_json_fname CPU_perf_16_threads_Uniform_PIP.json \
--overwrite 0 \
--cpp_exe_dir /mnt/scratch/wenqi/spatial-join-baseline/cpp/multithread \
--C_file_dir /mnt/scratch/wenqi/spatial-join-baseline/generated_data \
--num_threads 16 \
--parallel_approach bfs_dynamic \
--num_runs 3 \
--dataset Uniform \
--join_type Point-in-Polygon \
--log_name log_Uniform_PIP


python cpp_run_all_multithread_experiments.py \
--out_json_fname CPU_perf_16_threads_Uniform_PP.json \
--overwrite 0 \
--cpp_exe_dir /mnt/scratch/wenqi/spatial-join-baseline/cpp/multithread \
--C_file_dir /mnt/scratch/wenqi/spatial-join-baseline/generated_data \
--num_threads 16 \
--parallel_approach bfs_dynamic \
--num_runs 3 \
--dataset Uniform \
--join_type Polygon-Polygon \
--log_name log_Uniform_PP
```

Compare the performance of various BFS/BFS-DFS x dynamic/static parallel approaches
(using 10M datasets):

```
python cpp_run_all_multithread_experiments.py \
--out_json_fname CPU_perf_parallel_approaches_16_threads.json \
--overwrite 0 \
--cpp_exe_dir /mnt/scratch/wenqi/spatial-join-baseline/cpp/multithread \
--C_file_dir /mnt/scratch/wenqi/spatial-join-baseline/generated_data \
--dataset_size 10000000 \
--num_threads 16 \
--parallel_approach all \
--num_runs 1 \
--log_name log_parallel_approach
```

## Run all single-thread experiments

Either run the following one script (for a longer time):

```
python cpp_run_all_singlethread_experiments.py \
--out_json_fname CPU_perf_singlethread_sync_traversal.json \
--overwrite 0 \
--cpp_exe_dir /mnt/scratch/wenqi/spatial-join-baseline/cpp/singlethread \
--C_file_dir /mnt/scratch/wenqi/spatial-join-baseline/generated_data \
--max_entry 16 \
--num_runs 3 \
--log_name log_st
```

## Thread scalability tests

```
python cpp_run_all_multithread_experiments.py \
--out_json_fname CPU_perf_scalability_1_threads.json \
--overwrite 0 \
--cpp_exe_dir /mnt/scratch/wenqi/spatial-join-baseline/cpp/multithread \
--C_file_dir /mnt/scratch/wenqi/spatial-join-baseline/generated_data \
--dataset_size 10000000 \
--num_threads 1 \
--parallel_approach bfs_dynamic \
--num_runs 1

python cpp_run_all_multithread_experiments.py \
--out_json_fname CPU_perf_scalability_2_threads.json \
--overwrite 0 \
--cpp_exe_dir /mnt/scratch/wenqi/spatial-join-baseline/cpp/multithread \
--C_file_dir /mnt/scratch/wenqi/spatial-join-baseline/generated_data \
--dataset_size 10000000 \
--num_threads 2 \
--parallel_approach bfs_dynamic \
--num_runs 1

python cpp_run_all_multithread_experiments.py \
--out_json_fname CPU_perf_scalability_4_threads.json \
--overwrite 0 \
--cpp_exe_dir /mnt/scratch/wenqi/spatial-join-baseline/cpp/multithread \
--C_file_dir /mnt/scratch/wenqi/spatial-join-baseline/generated_data \
--dataset_size 10000000 \
--num_threads 4 \
--parallel_approach bfs_dynamic \
--num_runs 1

python cpp_run_all_multithread_experiments.py \
--out_json_fname CPU_perf_scalability_8_threads.json \
--overwrite 0 \
--cpp_exe_dir /mnt/scratch/wenqi/spatial-join-baseline/cpp/multithread \
--C_file_dir /mnt/scratch/wenqi/spatial-join-baseline/generated_data \
--dataset_size 10000000 \
--num_threads 8 \
--parallel_approach bfs_dynamic \
--num_runs 1

python cpp_run_all_multithread_experiments.py \
--out_json_fname CPU_perf_scalability_16_threads.json \
--overwrite 0 \
--cpp_exe_dir /mnt/scratch/wenqi/spatial-join-baseline/cpp/multithread \
--C_file_dir /mnt/scratch/wenqi/spatial-join-baseline/generated_data \
--dataset_size 10000000 \
--num_threads 16 \
--parallel_approach bfs_dynamic \
--num_runs 1

```

## Energy consumption experiments

The measure duration is around 2 minutes. 

First, measure the idle CPU energy consumption using an interval of 1 second:

```
/usr/lib/linux-tools-5.15.0-73/turbostat --S --interval 1 > log_energy_idle
```


The PkgWatt is the total energy consumption of two sockets. 

I measured the energy consumption on two 10M x 10M datasets, running bfs_dynamic approach, pinned on socket 0 using 16 cores. 

```
taskset --cpu-list 0-15 ../cpp/multithread_energy ../generated_data/C_OSM_10000000_polygon_file_0.txt ../generated_data/C_OSM_10000000_polygon_file_1.txt bfs_dynamic 16 16
taskset --cpu-list 0-15 ../cpp/multithread_energy ../generated_data/C_OSM_10000000_point_file_0.txt ../generated_data/C_OSM_10000000_polygon_file_0.txt bfs_dynamic 16 16
taskset --cpu-list 0-15 ../cpp/multithread_energy ../generated_data/C_uniform_10000000_polygon_file_0_set_0.txt ../generated_data/C_uniform_10000000_polygon_file_1_set_0.txt bfs_dynamic 16 16
taskset --cpu-list 0-15 ../cpp/multithread_energy ../generated_data/C_uniform_10000000_polygon_file_0_set_0.txt ../generated_data/C_uniform_10000000_point_file_0.txt bfs_dynamic 16 16
```

Once the index construction is finished and the join begins:

```
/usr/lib/linux-tools-5.15.0-73/turbostat --S --interval 1 > log_energy_OSM_10M_PP
/usr/lib/linux-tools-5.15.0-73/turbostat --S --interval 1 > log_energy_OSM_10M_PIP
/usr/lib/linux-tools-5.15.0-73/turbostat --S --interval 1 > log_energy_Uniform_10M_PP
/usr/lib/linux-tools-5.15.0-73/turbostat --S --interval 1 > log_energy_Uniform_10M_PIP
```

Get the energy consumption number:

```
python turbostat_energy_parsing.py --fname log_energy_idle
python turbostat_energy_parsing.py --fname log_energy_OSM_10M_PP 
```

Idle energy: 74.48 W (2 sockets); 

OSM_10M_PP Runtime energy: 146.77 W (2 sockets) -> **the working socket energy = 146.77 - 74.48 / 2 = 109.53 W**. 

OSM_10M_PIP Runtime energy: 181.93 W (2 sockets) -> **the working socket energy = 181.93 - 74.48 / 2 = 144.69 W**. This is not far away from the 155W TDP of AMD EPYC 7313 16-Core Processor.

Uniform_10M_PP Runtime energy: 173.70 W (2 sockets) -> **the working socket energy = 173.70 - 74.48 / 2 = 136.46 W**. This is not far away from the 155W TDP of AMD EPYC 7313 16-Core Processor.

Uniform_10M_PIP Runtime energy: 178.48 W (2 sockets) -> **the working socket energy = 178.48 - 74.48 / 2 = 141.24 W**. This is not far away from the 155W TDP of AMD EPYC 7313 16-Core Processor.

I tried different page sizes (increase to 64), and the result is pretty similar.

It seems that turbostat use the AMD RAPL:

```
CPUID(0): AuthenticAMD 0x10 CPUID levels
CPUID(1): family:model:stepping 0x19:1:1 (25:1:1) microcode 0x0
CPUID(0x80000000): max_extended_levels: 0x80000023
CPUID(1): SSE3 MONITOR - - - TSC MSR - HT -
CPUID(6): APERF, No-TURBO, No-DTS, No-PTM, No-HWP, No-HWPnotify, No-HWPwindow, No-HWPepp, No-HWPpkg, No-EPB
CPUID(7): No-SGX
RAPL: 234 sec. Joule Counter Range, at 280 Watts
/dev/cpu_dma_latency: 2000000000 usec (default)
current_driver: acpi_idle
current_governor: menu
current_governor_ro: menu
cpu51: POLL: CPUIDLE CORE POLL IDLE
cpu51: C1: ACPI FFH MWAIT 0x0
cpu51: C2: ACPI IOPORT 0x814
cpu51: cpufreq driver: acpi-cpufreq
cpu51: cpufreq governor: schedutil
cpufreq boost: 1
cpu0: MSR_RAPL_PWR_UNIT: 0x000a1003 (0.125000 Watts, 0.000015 Joules, 0.000977 sec.)
cpu16: MSR_RAPL_PWR_UNIT: 0x000a1003 (0.125000 Watts, 0.000015 Joules, 0.000977 sec.)
```