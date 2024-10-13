### Nested Join and Index Join Scripts

These scripts calculate intersections between objects defined in two files using NLJ and INLJ.

#### Nested Join

The `nested_join.py` script uses a nested loop approach to check for intersections.

##### Usage

```
python nested_join.py --filepathA <<<fileA>>> --filepathB <<<fileB>>>
```

- `--filepathA`: Path to the first file containing objects.
- `--filepathB`: Path to the second file containing objects.

##### Example

```
python nested_join.py --filepathA ../data/txt/uniform_1000_polygon_file_0_set_0.txt --filepathB ../data/txt/uniform_1000_polygon_file_1_set_0.txt
```

#### Index Join

The `index_join.py` script uses a spatial R-tree index to find intersections between objects from the first binary file and a spatial R-tree.

##### Usage

```
python index_join.py --filepathA <<<fileA>>> --filepathB <<<fileB>>>
```

- `--filepathA`: Path to the first binary file containing objects.
- `--filepathB`: Path to the R-tree file.

##### Example

```
python index_join.py --filepathA ../data/bin/C_uniform_1000_polygon_file_0_set_0.bin --filepathB ../data/tree/tree_C_uniform_1000_polygon_file_0_set_0.bin
```


