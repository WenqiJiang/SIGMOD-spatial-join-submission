# Version control

This file tracks the functionality and performance of single/multi PE spatial join.

## Single PE
### V1.0

The vanilla version of single PE, but already has all functionality. 
#### potential optimizations

1. The write functionalities uses a while loop thus cannot infer burst write.

Potential: add a write aggregation functionality, say there is N packages in the FIFO, so the write can be burst. 

#### Performance

Measure executor time.

* sample_tree_level_1_self_join_156 : 0.0138 ms @ 140 MHZ
* sample_tree_level_2_self_join_2090.bin : 0.170814 ms @ 140 MHZ
* sample_tree_level_3_self_join_19246.bin : 1.7227 ms @ 140 MHZ
* sample_tree_level_4_self_join_235112.bin : 21.9326 ms @ 140 MHZ

### V2.0

Optimize the write functionality, manually do write bursting, and the AXI burst length as max_write_burst_length=128 (which has been proved to be sufficient in my write microbenchmarks).

#### Performance

Measure executor time.

* sample_tree_level_1_self_join_156 : 0.00781429 ms @ 140 MHZ
* sample_tree_level_2_self_join_2090.bin : 0.0749929 ms @ 140 MHZ
* sample_tree_level_3_self_join_19246.bin : 0.915336 ms @ 140 MHZ
* sample_tree_level_4_self_join_235112.bin : 12.7479 ms @ 140 MHZ

Conclusion: 2X speedup over the while loop write version. 
### V2.1

Further improve write burst: using a long burst packet size of 1024 and a shorter AXI burst length of 64 to allow multiple write operations on the fly. 

#### Performance

Measure executor time.
* sample_tree_level_1_self_join_156 : 0.00779286 ms @ 140 MHZ
* sample_tree_level_2_self_join_2090.bin : 0.0729286 ms @ 140 MHZ
* sample_tree_level_3_self_join_19246.bin : 0.915279 ms @ 140 MHZ
* sample_tree_level_4_self_join_235112.bin : 12.7375 ms @ 140 MHZ

Conclusion: No improvement. Pretty much the same as V2.0.
## Multi PE

### V1.0 

The vanilla version of multiple PE, but already has all functionality. 

#### potential optimizations

1. The design consumes the result from all join PEs in a round robin manner. In each round, it does a round-robin check for all join PEs in order to get results. This is true for both result write and cache manager. However, if one PE has much more results to write than the others, the process will be slow because reading the PE producing the most result will cost N_PE times. 

Potential solution: if the FIFO is not empty, use a while loop to continuously consume the data. 

2. The write functionalities uses a while loop thus cannot infer burst write.

Potential: add a write aggregation functionality, say there is N packages in the FIFO, so the write can be burst. 

#### Performance

Measure executor time.

1 PE:
* sample_tree_level_1_self_join_156 : 0.0138071 ms @ 140 MHZ
* sample_tree_level_2_self_join_2090.bin : 0.219321 ms @ 140 MHZ
* sample_tree_level_3_self_join_19246.bin : 2.34314 ms @ 140 MHZ
* sample_tree_level_4_self_join_235112.bin : 30.8454 ms @ 140 MHZ

**Conclusion: has some overhead compared to single PE version implementation. When there are many pages, the performance can only achieve ~70% of the single PE version.**

8 PE:
* sample_tree_level_1_self_join_156 : 0.0157857 ms @ 140 MHZ
* sample_tree_level_2_self_join_2090.bin : 0.194357 ms @ 140 MHZ
* sample_tree_level_3_self_join_19246.bin : 1.85011 ms @ 140 MHZ
* sample_tree_level_4_self_join_235112.bin : 20.6116 ms @ 140 MHZ

**Conclusion: has some speedup compare to 1 PE, but not a lot probably due to (a) the inefficient result gather functionality (b) the selectivity is too low, such that most of the pairs are written to memory which caps the performance.**


### V1.1

Compare to 1.0, optimizing the round-robin read from join PEs. Basically, use a while to read from a PE, so if one PE has much more output than others, the writer / cache manager can keep reading from it. 

Measure executor time.

1 PE:
* sample_tree_level_1_self_join_156 : 0.0138 ms @ 140 MHZ
* sample_tree_level_2_self_join_2090.bin : 0.2079 ms @ 140 MHZ
* sample_tree_level_3_self_join_19246.bin : 2.18251 ms @ 140 MHZ
* sample_tree_level_4_self_join_235112.bin : 28.7811 ms @ 140 MHZ

**Conclusion: better than V1.0, but still has some overhead compared to single PE version implementation. When there are many pages, the performance can only achieve ~76% of the single PE version.**


8 PE:
* sample_tree_level_1_self_join_156 : 0.0158286 ms @ 140 MHZ
* sample_tree_level_2_self_join_2090.bin : 0.173543 ms @ 140 MHZ
* sample_tree_level_3_self_join_19246.bin : 1.68542 ms @ 140 MHZ
* sample_tree_level_4_self_join_235112.bin : 21.2492 ms @ 140 MHZ

**Conclusion: has some speedup compare to 1 PE, but not a lot probably due to the selectivity is too low, such that most of the pairs are written to memory which caps the performance. The gather functionality should not be the problem anymore.**


### V2.0

Compare to 1.0, added the burst write functionality, and the AXI burst length as max_write_burst_length=128 (which has been proved to be sufficient in my write microbenchmarks).

1 PE:
* sample_tree_level_1_self_join_156 : 0.00977143 ms @ 140 MHZ
* sample_tree_level_2_self_join_2090.bin : 0.120871 ms @ 140 MHZ
* sample_tree_level_3_self_join_19246.bin : 1.46261 ms @ 140 MHZ
* sample_tree_level_4_self_join_235112.bin : 20.2549 ms @ 140 MHZ

**Conclusion: better than V1.0, ~1.5X speedup.**

8 PE:
* sample_tree_level_1_self_join_156 : 0.00979286 ms @ 140 MHZ
* sample_tree_level_2_self_join_2090.bin : 0.0674714 ms @ 140 MHZ
* sample_tree_level_3_self_join_19246.bin : 0.719693 ms @ 140 MHZ
* sample_tree_level_4_self_join_235112.bin : 8.99693 ms @ 140 MHZ

**Conclusion: better than V1.0, ~2X speedup; better than 1 PE, ~2X speedup -> but not 8X, probably need to use larger workloads to examine the performance.**
### V2.1

Compare to 2.0, further optimize the burst write functionality (wait until either a page join is finished or until the results are enough for a full burst).


1 PE:
* sample_tree_level_1_self_join_156 : 0.00980714 ms @ 140 MHZ
* sample_tree_level_2_self_join_2090.bin : 0.120879 ms @ 140 MHZ
* sample_tree_level_3_self_join_19246.bin : 1.46091 ms @ 140 MHZ
* sample_tree_level_4_self_join_235112.bin : 20.2526 ms @ 140 MHZ
* sample_tree_level_5_self_join_5182308.bin: 313.612 ms @ 140 MHZ

**Conclusion: essentially the same as V2.0.**

8 PE:
* sample_tree_level_1_self_join_156 : 0.00982143 ms @ 140 MHZ
* sample_tree_level_2_self_join_2090.bin : 0.0614286 ms @ 140 MHZ
* sample_tree_level_3_self_join_19246.bin : 0.649243 ms @ 140 MHZ
* sample_tree_level_4_self_join_235112.bin : 8.88854 ms @ 140 MHZ
* sample_tree_level_5_self_join_5182308.bin: 147.577 ms @ 140 MHZ

**Conclusion: essentially the same as V2.0. The speedup over 1 PE version is always slightly more than 2X, no matter the workload, so there is some scalability issue here.**

### V2.2

Compare to 2.1: 
* Add a monitor detecting the number of pages allocated to each PE.
* Fix the write address, the previous version writes from addr 0 which will be overwritten by result count...


8 PE:
* sample_tree_level_5_self_join_5182308.bin: 147.552 ms @ 140 MHZ

PE 0 computes 12039 page joins
PE 1 computes 11999 page joins
PE 2 computes 11734 page joins
PE 3 computes 11501 page joins
PE 4 computes 11562 page joins
PE 5 computes 11522 page joins
PE 6 computes 11422 page joins
PE 7 computes 11398 page joins

**Conclusion: the workloads are quite balanced across PEs -> so they are indeed busy most of the time (otherwise PE 0 will be dispatched to the job). So there must be something else preventing the scalability, e.g., is the write still too intensive?**

An estimation: 5182308 / (12039 * 8 * 256) = 21\% -> this does not even count the intermediate result writes. So having a higher selectivity will improve the performance much more.

Writing 5182308 results, if we can achieve 1 write per cycle, consume 5182308 / (140 * 1e6) = 0.037 sec = 37 ms, without considering intermediate result writes. 

### V2.3

Further improve write burst: using a long burst packet size of 1024 and a shorter AXI burst length of 64 to allow multiple write operations on the fly. 

8 PE:

* sample_tree_level_3_self_join_19246.bin : 0.649321 ms @ 140 MHZ
* sample_tree_level_4_self_join_235112.bin : 8.88628 ms @ 140 MHZ
* sample_tree_level_5_self_join_5182308.bin: 137.24 ms @ 140 MHZ

**Conclusion: no improvement after optimizing write. The problem could very well be the read: the read unit has to handle too many pages, because each only contains too little content (16 MBRs per page in the current workload). Plus, the layer cache has to fetch data pretty frequently from memory.**


### V2.4

Optimize the layer cache, add a BRAM-based cache in the scheduler. 

Original need:
* optimizize scheduler protocol: it should cache a number of page pairs to assign next, rather than issuing a lot of read request to the cache manager.

8 PE:

* sample_tree_level_3_self_join_19246.bin : 0.649207 ms @ 140 MHZ
* sample_tree_level_4_self_join_235112.bin : 8.89006 ms @ 140 MHZ
* sample_tree_level_5_self_join_5182308.bin: 137.142 ms @ 140 MHZ

Placement & Routing

16 PE, both in SLR2, all connected to DRAM channel 2 -> fail

16 PE, using two channels (2 and 3, read A 2 read B 2 write intermediate 2 write final 3) -> suceed

### V2.5

1. Shrink the MAX_PAGE_ENTRY_NUM (max page size in join PE) from 4096 to 512, reducing URAM usage (previous 14 URAM per PE). 
2. Optimize read. Previous: read the node meta first, then read content and parse (3 cycles per 64-byte). For small pages, this two random access with dependency can ruin performance. Current solution: set a software max entry number, the reader reads the entire node, sends the entire raw pages to the join PE, which is responsible for further parsing.

### V2.6

V2.5 merges the join PE with parsing the raw 512-byte page inputs. However, this lead to significant slow down in performance. This also lead to frequency drop at 200 MHz (achieved 169), which does not appear in V2.4 (either due to the more complex join PE, or because the reduced usage of URAM and increasing usage in BRAM due to the reduced hardware page size limit).

* In V2.6, I add a parser before each join PE, such that the PE can still focus on the join by consuming already parsed data.
* I connect two node input to the same DRAM bank, and the two result writing to the same DRAM bank. Such that read is not disturbed by writes (the two writes does not happen simultaneously). 
* Updated host to take input arguments. 

A potential bug to fix:
```
		// should be || rather than &&
        if (!s_page_A_raw.empty() && !s_page_B_raw.empty()) {
        else if (!s_join_finish_in.empty()) {
```

V2.6 Performance Analysis:

Tested on PIP on OSM, which yields very little results.

python perf_test.py \
--FPGA_project_dir /mnt/scratch/wenqi/spatial-join-on-FPGA/FPGA_BFS/BFS_multi_PE_v2.6_16_PE_250_2 \
--cpp_exe_dir /mnt/scratch/wenqi/spatial-join-baseline/cpp_old/a.out \
--get_tree_depth_py_dir /mnt/scratch/wenqi/spatial-join-baseline/python/get_tree_depth.py \
--C_file_A /mnt/scratch/wenqi/spatial-join-baseline/generated_data/C_OSM_10000000_point_file_0.txt \
--C_file_B /mnt/scratch/wenqi/spatial-join-baseline/generated_data/C_OSM_10000000_polygon_file_0.txt \
--max_entry_size 16 --num_runs 1

Page size = 64
	805.61 ms
	PE 0 computes 27973 page joins
	27973 / 0.805 = 34749 pages per sec
	theoretical perf 1 /  (64 * 64 / (206 * 1e6)) = 50292 pages per sec, so maybe still some space for improvement
	is this because of DRAM input or the task scheduler?
	DRAM input: 27973 * 16 * 4096 / 1e9 = 1.8 GB/s per input channel, seems this is not the bottleneck
	Read performance: suppose a read latency = 20 cycles, 64 entry ~= 1 header + 22 64-byte = 23 cycles -> 43 cycles, theoretical read performance = 1 /  (43  / (206 * 1e6)) = 4790697 -> per PE: 4790697 / 16 = 299,418 -> still 6x more than the per PE performance	
	maybe increase pair_cache_size from 32 -> ??? 32 only last for two rounds of assignment, but setting it to something as large as 1024 can also create idle bubble for join units to wait this read finish -> but seems to be fine overall

Page size = 16
	2154.33 ms
	PE 0 computes 103230 page joins
	103230 / 2.154 = 47,924 pages per sec <<< theoretical perf 1 /  (16 * 16 / (206 * 1e6)) 804,687 -> 20x compute gap
	Read performance: suppose a read latency = 20 cycles, 16 entry ~= 1 header + 6 64-byte = 7 cycles -> 27 cycles, theoretical read performance = 1 /  (27  / (206 * 1e6)) = 7,629,629 -> per PE: 7629629 / 16 = 476,851 -> still 10x more than the per PE performance	

Performance conclusion: for small page size of 16, it is far from achieving FPGA's max bandwidth, even if we count the read latency. This means there are bubbles in the read-join-write pipeline such that the performance is not maximized. For larger page size of 64, it achieves around 65% of theoretical compute performance, meaning that the pipeline bubble is still there.


**Known Issue: results = 0 when Tree depth A > Tree depth B, so we should always make sure we start with the smaller or equal sized dataset first**

Example:

1M point x 100K polygon, max entry =64 -> bug, FPGA return 0

1 PE, 2 PE, 16 PE -> all 0 results

python perf_test.py  --FPGA_project_dir /mnt/scratch/wenqi/spatial-join-on-FPGA/FPGA_BFS/BFS_multi_PE_v2.6_1_PE  --FPGA_host_name host  --FPGA_bin_name xclbin/vadd.hw.xclbin  --FPGA_log_name summary.csv  --cpp_exe_dir /mnt/scratch/wenqi/spatial-join-baseline/cpp/a.out  --C_file_A /mnt/scratch/wenqi/spatial-join-baseline/generated_data/C_uniform_1000000_point_file_0.txt --C_file_B /mnt/scratch/wenqi/spatial-join-baseline/generated_data/C_uniform_100000_polygon_file_0_set_0.txt --get_tree_depth_py_dir /mnt/scratch/wenqi/spatial-join-baseline/python/get_tree_depth.py  --max_entry_size 64  --num_runs 1  > log_perf_test

level A: 4
level B: 3

### V2.7

* Add input tree depth check, depth A must <= depth B.
* Use a max work count per join PE, rather than waiting for the PE to finish until the next task assignment can begin.
* Increase the task cache from 32 to 128 (the cache load latency is covered by the busy join PEs)
// * Increase data FIFO size from 512 to 1024 -> rolled back, routing error
// * Increase the between-kernel FIFO size in the vivado.cfg file from 64 to 1024 -> rolled back, routing error
* Fix `if (!s_page_A_raw.empty() && !s_page_B_raw.empty())` -> `if (!s_page_A_raw.empty() || !s_page_B_raw.empty())`

Results:
* Limited performance improvement over 2.6 -> Probably because the scheduling logic is too complex, thus taking a lot of cycles to send tasks out. 
* When the number of tasks is large, it can run into deadlock -> still not sure why

```
16, max activa task 1 = 0: 2734.43 ms
16, max activa task 2 = 2561.69 ms
16, max activa task 3 = 2630.41 ms
16, max activa task 4 -> deadlock?

32, max activa task 1 = 1382.52 ms
32, max activa task 2 = 1325.33 ms

64, max activa task 1 = 956.53 ms
64, max activa task 2 = 922.97 ms
```

### V2.8

In 2.7, we inspect that the dynamic scheduling policy is a big overhead in the scheduler, resulting in insufficient workload per join PE. Thus, in 2.8, we use a static policy instead to assign exactly the same number of tasks to each PE. Given that all the non-root-level nodes should be mostly full (each join task has similar workload), the imbalance time consumption per PE should be minimal. 

* rewrite scheduler
* remove the PE idle signal

Performance: 

1 PE, page size = 16:

	python perf_test.py --FPGA_project_dir /mnt/scratch/wenqi/spatial-join-on-FPGA/FPGA_BFS/BFS_multi_PE_v2.8_1_PE --cpp_exe_dir /mnt/scratch/wenqi/spatial-join-baseline/cpp_old/a.out --get_tree_depth_py_dir /mnt/scratch/wenqi/spatial-join-baseline/python/get_tree_depth.py --C_file_A /mnt/scratch/wenqi/spatial-join-baseline/generated_data/C_OSM_10000000_point_file_0.txt --C_file_B /mnt/scratch/wenqi/spatial-join-baseline/generated_data/C_OSM_10000000_polygon_file_0.txt --max_entry_size 16 --num_runs 1

	PE 0 computes 1651630 page joins
	Run 0 FPGA end-to-end: 3195.67 ms
	Run 0 FPGA kernel: 3191.22 ms

	1651630 / 3.191 = 517,590 pages per sec; theoretical perf 1 / ((16 * 16 + 16 + 73) / (200 * 1e6)) = 579,710 -> very close
	Read performance: suppose a read latency = 50 cycles, 16 entry ~= 1 header + 6 64-byte = 7 cycles -> 57 cycles, theoretical read performance = 1 /  (57  / (200 * 1e6)) = 3,508,771 -> per PE: 3,508,771 / 1 = 3,508,771 -> can support up to 6 PEs

2 PE, page size = 16: 2164.98 ms
	-> already not very scalable! Thus the scheduling policy is not the reason? What's the reason then? Is the read unit's burst read, or the feedback loop per layer that yields extra coordination overhead? Or the static scheduling itself is bad?

8 PE, page size = 16: 2166.98 ms

### V2.9

In V2.8, we inspect that the read unit is actually not optimized, either due to the AXIS interface or somehow burst is not inferred. In V2.9 we handle AXIS signal in another PE, and use an individual PE to select join unit.

My suspiction is that the old code reading simultaneously from 2 DRAM bank will not lead to burst read:

```
	for (int i = 0; i < axi_per_page; i++) {
#pragma HLS pipeline II=1
		s_page_A_raw[join_PE_ID].write(in_pages_A[start_addr_A + i]);
		s_page_B_raw[join_PE_ID].write(in_pages_B[start_addr_B + i]);
		}
```

Performance: Limited improvement over 2.8.
### V2.10 

Forked from V2.8. Using burst read AXI and prefetch multiple pages. 

Performance: Limited improvement over 2.8. 2027.55 ms for 16 PE, 16 page size, OSM PIP 10M x 10M.

Analysis: maybe the bottleneck is not on the read side, but on the write size (layer cache controller and write manager).

I used tracked some data in the cpp bfs for OSM PIP 10M x 10M:

Building RTree for trace 1: 10717.19 ms
Building RTree for trace 2: 13169.71 ms
New level size: 19
New level size: 365
New level size: 15368
New level size: 276958
New level size: 1358919
New level size: 0
BFS + dynamic duration: 894.54 ms
Number of results (BFS + dynamic): 12407

So there are actually quite some write operations

but even if we consider all the writes, each in individual random write:

(19 + 365 + 15368 + 276958 + 1358919 + 12407) * (300 * 1e-9) = 0.5 s -> not 2 sec

### V2.11

Upon V2.10, improve write functionality by using manual write bursts in both layer controller and write unit. 

1Mx1M OSM PIP:
1 PE, page size = 16, 307.1 ms
PE 0 computes 155844 page joins -> 155844 / 0.307 = 507,635 tasks / sec
2 PE, page size = 16, 198.83 ms
computes 155844 page joins -> 155844 / 0.198 = 787,090 tasks / sec

1 PE, page size = 8, 317.27 ms
PE 0 computes 317633 page joins -> 317633 / 0.317 = 1,001,996 tasks / sec
2 PE, page size = 8, 318.04 ms -> same tasks / sec

1 PE, page size = 4, 655.78 ms
PE 0 computes 550423 page joins -> 550423 / 0.65 =  846,804 tasks / sec
2 PE, page size = 8, 654.15 ms -> same tasks / sec

**So probably ~1M tasks / sec is the limit of the FPGA control logic (200 cycles / task which still sounds quite high). Question, what is the thing in the pipeline that creates this 200 cycle overhead? Read/write, or other stuff?**

**Also, adding more PEs do not degrade performance probably means that looping over these PEs in a control logic is not the bottleneck -> then the bottleneck should still be read or write, and given that the read, in the microbenchmark, should consume less than 100 cycles, the write could be the bottleneck**

page size = 32, 10Mx10M OSM -> 843,162 tasks -> probably needs around 0.8 sec. -> in reality it takes 800.54 ms

page size = 48, 10Mx10M OSM -> 572,208 tasks -> probably needs around 0.6 sec. -> in reality it takes 704.173 ms

page size = 64, 10Mx10M OSM -> 447,568 tasks -> probably needs around 0.5 sec., but compute takes more -> in reality it takes 791.81 ms

### V2.12 

Compare to V2.11:
* Further optimize write by combining multiple writes into a single write, given that we write data into consecutive addresses.
* Increase task cache size from 128 to 1024, as well as the inter-kernel signals (axis_page_pair_scheduler, axis_page_ID_pair_read_nodes, axis_join_PE_ID)
* Increase the s_meta FIFO size from 8 to 512

Perf:
very similar 1Mx1M OSM PIP 1 PE performance to V2.11 -> thus write should not be on the critical path, and something else is the 200 cycle critical path in the pipeline.

To see which stage is the bottleneck, I tried two thing:

1. BFS_multi_PE_v2.12_2_PE_more_write_channels, use different channels for write unit and layer cache manager -> no improvement.

2. BFS_multi_PE_v2.12_2_PE_less_read_channels, use only one read channel to see if performance decrease by the random access latency. 

Before: 653.02 ms (550423 pages), now 842.69 ms, suppose latency = 300 ns. 653.02 + 300 * 1e-6 * 550423 = 653 + 165 = 818 ms. **So indeed read is the bottleneck, because if there is another outstanding bottleneck in the pipeline, the increased read latency can be hided. But given that random read only consumes 300 ns, why in reality, it's so slow?**


Change the read/write latency:

If we set latency = 0, this means default = 64. 

(default latency) 1 PE, page size = 16, 307.1 ms
PE 0 computes 155844 page joins -> 155844 / 0.307 = 507,635 tasks / sec
(default latency) 2 PE, page size = 16, 198.83 ms
computes 155844 page joins -> 155844 / 0.198 = 787,090 tasks / sec
(latency = 32)    2 PE, page size = 16, 197.35 ms -> somehow no improvement here, bottleneck is elsewhere? maybe the layer scheduler? 
(latency = 16)    2 PE, page size = 16, 197.43 ms -> somehow no improvement here, bottleneck is elsewhere?
(latency = 16 reorder join PE ID read and mem channel)    2 PE, page size = 16, 198.76 ms -> same as normal latency = 16
(latency = 1 only for write)     2 PE, page size = 16, 198.49 ms  -> same as all others
(latency = 1 only for layer cache)     2 PE, page size = 16, 199.59 ms  -> same as all others
(latency = 1 only for read)    2 PE, page size = 16,  198.34 ms -> a little bit slower than all latency = 1
(latency = 1)    2 PE, page size = 16,  197.74 ms -> somehow no improvement here, bottleneck is elsewhere?
(latency = 1)    16 PE @189 MHz, page size = 16, 107.0 ms -> somehow this is faster than 2 PEs -> maybe due to merged writes? or maybe more PEs means longer total FIFO sizes?  Also, this achieves 155844 / 0.107 = 1456485 tasks per sec -> 130 cycles per task

(default latency) 1 PE, page size = 8, 317.27 ms
PE 0 computes 317633 page joins -> 317633 / 0.317 = 1,001,996 tasks / sec
(default latency) 2 PE, page size = 8, 318.04 ms -> same tasks / sec
(latency = 32)    2 PE, page size = 8, 264.524 ms -> 32 cycles reduced = 317633 * 32 * 5e-9 = 50.8 ms, indeed this is the delta. 
(latency = 16 reorder join PE ID read and mem channel)    2 PE, page size = 8, 252.44 ms -> same as normal latency = 16
(latency = 16)    2 PE, page size = 8, 250.73  ms -> 48 cycles reduced = 317633 * 48 * 5e-9 = 76.2 ms, indeed this is (almost) the delta. 
(latency = 1 only for write)     2 PE, page size = 8, 317.89 ms -> same as default latency -> writer is not on the critical path
(latency = 1 only for layer cache)     2 PE, page size = 8, 318.55 ms -> same as default latency -> layer cache is not on the critical path
(latency = 1 only for read)     2 PE, page size = 8, 250.4 ms -> same as all latency = 1
(latency = 1)     2 PE, page size = 8, 250.98  ms -> no further improvement upon 16? -> latency somewhere else, not read? 
(latency = 1)     16 PE @189 MHz, page size = 8, 219.37 ms -> somehow this is faster than 2 PEs?

(default latency) 1 PE, page size = 4, 655.78 ms
PE 0 computes 550423 page joins -> 550423 / 0.65 =  846,804 tasks / sec
(default latency) 2 PE, page size = 4, 654.15 ms -> same tasks / sec
(latency = 32)    2 PE, page size = 4, 564.622 ms ->  32 cycles reduced = 550423 * 32 * 5e-9 = 88.0 ms, indeed this is the delta. 
(latency = 16 reorder join PE ID read and mem channel)    2 PE, page size = 4, 525.22 ms ->  same as normal latency = 16
(latency = 16)    2 PE, page size = 4, 522.95 ms ->  48 cycles reduced = 550423 * 48 * 5e-9 = 132.1 ms, indeed this is the delta. 
(latency = 1 only for write)     2 PE, page size = 4, 654.47 ms ->same as default latency -> writer is not on the critical path
(latency = 1 only for layer cache)     2 PE, page size = 4, 666.37 ms -> even slower than default latency -> layer cache is not on the critical path
(latency = 1 only for read)     2 PE, page size = 4, 489.56 ms -> seems slower than all latency=1 
(latency = 1)     2 PE, page size = 4, 480.903 ms -> 63 cycles reduced = 550423 * 64 * 5e-9 = 176.1 ms, indeed this is the delta. 
(latency = 1)     16 PE @189 MHz, page size = 4, 485.19 ms -> not faster than 2 PE


So setting latency improved the performance where we use super small page sizes (e.g., 4) -> does this mean something else is on the way when using larger page sizes?

I then read the summary.csv. latency = 1, 2 PE, page size = 8, 158819 + 158814 = 317633 tasks. Essentially there is no burst read. On the otherhand, the average memory access latency is only 56.4944 ns. 

On interesting fact: why executor only consumes 65.1982 ms, while the scheduler consumes 250.018 ms? executor 65.1982 ms -> 4,871,806 tasks per sec, which is quite good.

```
Compute Unit Utilization
Device,Compute Unit,Kernel,Global Work Size,Local Work Size,Number Of Calls,Dataflow Execution,Max Overlapping Executions,Dataflow Acceleration,Total Time (ms),Minimum Time (ms),Average Time (ms),Maximum Time (ms),Clock Frequency (MHz),
xilinx_u250_gen3x16_xdma_shell_4_1-0,executor_1,executor,1:1:1,1:1:1,1,Yes,1,1.000000x,65.1982,65.1982,65.1982,65.1982,200,
xilinx_u250_gen3x16_xdma_shell_4_1-0,scheduler_1,scheduler,1:1:1,1:1:1,1,Yes,1,1.000000x,250.018,250.018,250.018,250.018,200,

Data Transfer: Kernels to Global Memory
Device,Compute Unit/Port Name,Kernel Arguments,Memory Resources,Transfer Type,Number Of Transfers,Transfer Rate (MB/s),Average Bandwidth Utilization (%),Average Size (KB),Average Latency (ns),
xilinx_u250_gen3x16_xdma_shell_4_1-0,executor_1/m_axi_gmem0,in_pages_A,DDR[0],READ,317633,3144.1,16.333,0.256,56.4944,
xilinx_u250_gen3x16_xdma_shell_4_1-0,executor_1/m_axi_gmem1,in_pages_B,DDR[2],READ,317633,3213.13,16.6916,0.256,45.2185,
xilinx_u250_gen3x16_xdma_shell_4_1-0,executor_1/m_axi_gmem2,layer_cache,DDR[3],WRITE,32081,482.549,2.50675,0.079,38.3504,
xilinx_u250_gen3x16_xdma_shell_4_1-0,executor_1/m_axi_gmem2,layer_cache,DDR[3],READ,20167,1536.14,7.97994,0.126,213.201,
xilinx_u250_gen3x16_xdma_shell_4_1-0,executor_1/m_axi_gmem3,out_intersect,DDR[3],WRITE,382,74.9471,0.389335,0.011,29.7408,
```

I figured out the problem!!! So the bottleneck is actually data movement. Previously I didn't insert q.finish() between input data movement and starting the kernel, thus the scheduler already starts without having memcpy finished, leading to a lot of wasted cycles, and my kernel time recorder also select the max kernel time of the two. In fact, the kernel time of OSM PIP 10M only consumes <200 ms while the entire run takes 619 ms. 

Here is the debugging log: 

200 MHz

Duration (including memcpy out): 635.22 msDuration (kernel): 188.76 ms
Result correct!
Parsed intersect count : 12407
FPGA computed intersect count : 12407
In total, all PEs compute 1651630 page joins

total read: 1651630
total writes on layer cache: up to 1651630 - 1 writes = 1651629
total writes on write unit: 12407

Reads in the log:
page read: 1651630 -> indeed, we read 1651630 stuff, they may be bursted, but still considered as individual read -> if one read takes 100 ns -> 1651630 * 1e-7 = 0.165 s, close to the kernel performance.
layer cache write: 112935 -> burst + some task yields no results, should not be the bottleneck given that the number of writes << the number of page read
write unit write: 5752 -> there are burst

Seems DDR0 has quite a long latency: probably due to its position relative to the kernel. -> reordering the memory channel will indeed improve performance here, i.e., even using latency = 16 lead to only 53 ns latency on channel 0 and 1


### V3.1 

I call this a major update as it supports various page sizes, passing as an input argument, thus the data movement cost on PCIe can be minimized. 

1~8 PE: V3.2 -> same some control signal FIFOs
16 PE: V3.1 -> using shift register to implement FIFOs can lead to routing errors

3.1 inherited from BFS_multi_PE_v2.12_16_PE_latency_1_fix_burst_param, with minor adjustments:
* the join_PE_ID read in the read unit is moved to after the read request is sent
* use two different strategies of DDR channel mapping (read from 3, 2; layer cache / read: 0)
* host support various page size (previously fixed to 1024)
* AXI latency always set to 1


Memory channel mapping A: read -> 0, 2; write / layer cache: 3
Run 0 FPGA end-to-end: 253.36 ms
Run 0 FPGA kernel: 201.77 ms
Read channels latency: 49~55 ns

Memory channel mapping B: read -> 3, 2; write / layer cache: 0
Run 0 FPGA end-to-end: 242.37 ms
Run 0 FPGA kernel: 191.03 ms -> 5% better than the first mapping
Read channels latency: 50~51 ns -> 10% better than the first mapping

### V3.2 optimize BRAM usage

This is for the resource consumption section. The 16 PE version of V3.1 consume 32% of BRAM in the kernels. 

1~8 PE: V3.2 -> same some control signal FIFOs
16 PE: V3.1 -> using shift register to implement FIFOs can lead to routing errors. I tried several versions of P&R hints but none of them worked. 

Here is the FIFO BRAM consumptions of a single PE in V3.1:

```
    * FIFO:
    +------------------------------------------------+---------+------+----+-----+------+-----+---------+
    |                      Name                      | BRAM_18K|  FF  | LUT| URAM| Depth| Bits| Size:D*B|
    +------------------------------------------------+---------+------+----+-----+------+-----+---------+
    |layer_cache_c_U                                 |        0|     7|   0|    -|    10|   64|      640|
    |max_entry_num_c_U                               |        0|     5|   0|    -|     3|   32|       96|
    |max_level_A_c_U                                 |        0|     7|   0|    -|    10|   32|      320|
    |max_level_B_c_U                                 |        0|     7|   0|    -|    10|   32|      320|
    |out_intersect_c_U                               |        0|     7|   0|    -|    11|   64|      704|
    |root_id_A_c_U                                   |        0|     7|   0|    -|    10|   32|      320|
    |root_id_B_c_U                                   |        0|     7|   0|    -|    10|   32|      320|
    |s_finish_join_PE_out56_U                        |        0|    68|   0|    -|     2|   32|       64|
    |s_finish_join_PE_out_aggregated_U               |        0|    68|   0|    -|     2|   32|       64|
    |s_finish_layer_cache_burst_out57_U              |        0|    68|   0|    -|     2|   32|       64|
    |s_finish_layer_cache_out_U                      |        0|    68|   0|    -|     2|   32|       64|
    |s_finish_parse_page_out55_U                     |        0|    68|   0|    -|     2|   32|       64|
    |s_finish_read_out_U                             |        0|    68|   0|    -|     2|   32|       64|
    |s_finish_read_out_replicated54_U                |        0|    68|   0|    -|     2|   32|       64|
    |s_finish_scheduler_out_U                        |        0|    68|   0|    -|     2|   32|       64|
    |s_finish_write_burst_out58_U                    |        0|    68|   0|    -|     2|   32|       64|
    |s_intersect_count_directory44_U                 |        1|    95|   0|    -|   512|   32|    16384|
    |s_intersect_count_leaf46_U                      |        1|    95|   0|    -|   512|   32|    16384|
    |s_meta_A40_U                                    |        8|   543|   0|    -|   512|  224|   114688|
    |s_meta_B41_U                                    |        8|   543|   0|    -|   512|  224|   114688|
    |s_page_A42_U                                    |        8|   543|   0|    -|   512|  160|    81920|
    |s_page_A_raw38_U                                |       15|  1056|   0|    -|   512|  512|   262144|
    |s_page_B43_U                                    |        8|   543|   0|    -|   512|  160|    81920|
    |s_page_B_raw39_U                                |       15|  1056|   0|    -|   512|  512|   262144|
    |s_result_pair_directory45_U                     |        4|   287|   0|    -|  1024|   65|    66560|
    |s_result_pair_directory_burst50_U               |        4|   287|   0|    -|  1024|   65|    66560|
    |s_result_pair_directory_burst_contain_last49_U  |        1|    95|   0|    -|   512|   32|    16384|
    |s_result_pair_directory_burst_length48_U        |        1|    95|   0|    -|   512|   32|    16384|
    |s_result_pair_leaf47_U                          |        4|   287|   0|    -|  1024|   65|    66560|
    |s_result_pair_leaf_burst53_U                    |        4|   287|   0|    -|  1024|   65|    66560|
    |s_result_pair_leaf_burst_contain_last52_U       |        1|    95|   0|    -|   512|   32|    16384|
    |s_result_pair_leaf_burst_length51_U             |        1|    95|   0|    -|   512|   32|    16384|
    +------------------------------------------------+---------+------+----+-----+------+-----+---------+
    |Total                                           |       84|  6661|   0|    0| 10322| 2820|  1285344|
    +------------------------------------------------+---------+------+----+-----+------+-----+---------+
```

Candidate for optimizations:
s_meta_A, s_meta_B -> len 512 to 32; BRAM 8 -> 0
s_page_A, s_page_B  -> len 512 to 256; BRAM still 8 
s_page_A_raw, s_page_B_raw -> len 512 to 128; BRAM still 15 

s_result_pair_directory -> len 1024 to 512; BRAM still 4 
s_result_pair_directory_burst -> len 1024 to 512; BRAM still 4
s_result_pair_directory_burst_length -> len 512 to 32; BRAM 1 -> 0
s_result_pair_directory_burst_contain_last -> len 512 to 32; BRAM 1 -> 0
s_intersect_count_directory -> len 512 to 32; BRAM 1 -> 0

s_result_pair_leaf -> len 1024 to 512; BRAM still 4 
s_result_pair_leaf_burst -> len 1024 to 512; BRAM still 4 
s_result_pair_leaf_burst_length -> len 512 to 32; BRAM 1 -> 0
s_result_pair_leaf_burst_contain_last -> len 512 to 32; BRAM 1 -> 0
s_intersect_count_leaf -> len 512 to 32; BRAM 1 -> 0

Conclusion: changing len 512 or 1024 to shorter does not help resource consumption, but we can choose to save the resources for those with only control signals. So we keep them as 512 in the end. 
#### Potential further optimization

* change the workload -> it should join pages of at least hundreds of entries, not using the current workload with just 16 MBRs. 
* using PCIe streaming interface (not supported anymore in Vitis) or network to send out the results. Some relevant material that may help improving memory copy:
  * https://support.xilinx.com/s/question/0D52E00006vy1fHSAQ/how-to-stream-data-between-host-and-kernel?language=en_US
  * https://github.com/Xilinx/Vitis_Accel_Examples/blob/master/host/host_memory_simple/src/host.cpp
  * Not sure those memory mapped region on host will be automatically updated once the FPGA writes something