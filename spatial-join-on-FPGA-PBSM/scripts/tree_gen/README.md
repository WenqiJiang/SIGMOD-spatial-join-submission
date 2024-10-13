# R-tree Visualisation Script

This script reads and visualises an R-tree from a bin file. It allows to specify the file path and the max number of entries in a node.

## Usage

### Running the script

To run the script, use the following command:

```bash
python visualise_tree.py <file_name> <max_entries>
```

- `<file_name>`: Path to the binary file.
- `<max_entries>`: Maximum entries allowed in each node.

## Example

```bash
python visualise_tree.py "../../data/tree/tree_uniform_1000_polygon_file_0_set_0.bin" 16
```
