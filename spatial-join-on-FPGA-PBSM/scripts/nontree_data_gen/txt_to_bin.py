import argparse
import struct


class ObjT:
    def __init__(self, id, low0, high0, low1, high1):
        self.id = id
        self.low0 = low0
        self.high0 = high0
        self.low1 = low1
        self.high1 = high1


def pad_to_64_bytes(data):
    padding_length = 64 - (len(data) % 64)

    # no padding needed (not really used but just in case)
    if padding_length == 64:
        return data

    # pad with zeros
    return data + b'\x00' * padding_length


def read_txt(txt_file_path):
    objects = []

    with open(txt_file_path, 'r') as file:
        next(file)

        for line in file:
            parts = line.split()

            # read only rows that
            if len(parts) == 5:
                obj = ObjT(int(parts[0]), float(parts[1]), float(parts[2]), float(parts[3]), float(parts[4]))
                objects.append(obj)

    return objects


def write_binary_file(objects, binary_file_path):
    with open(binary_file_path, 'wb') as file:
        num_objects_data = struct.pack('i', len(objects))
        padded_num_objects_data = pad_to_64_bytes(num_objects_data)
        # write number of objects (padded)
        file.write(padded_num_objects_data)

        # write pages of objects, each 'page' is a group of 3 objects
        data = b''
        for i, obj in enumerate(objects):
            # pack: int, float, float, float, float
            obj_data = struct.pack('iffff', obj.id, obj.low0, obj.high0, obj.low1, obj.high1)

            # collect binary data
            data += obj_data

            # add padding and write (after every 3rd obj)
            if (i + 1) % 3 == 0 or i == len(objects) - 1:
                padded_data = pad_to_64_bytes(data)
                file.write(padded_data)

                # reset the collector
                data = b''


def convert_txt_to_bin(txt_filepath, bin_filepath):
    objects = read_txt(txt_filepath)
    write_binary_file(objects, bin_filepath)


# Example usage
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Convert TXT files to BIN format.")
    parser.add_argument('--txt_files', nargs='+', help="List of TXT file paths", required=True)
    parser.add_argument('--bin_files', nargs='+', help="List of corresponding BIN file paths", required=True)

    args = parser.parse_args()

    if len(args.txt_files) != len(args.bin_files):
        raise ValueError("The number of txt files and bin files must match.")

    for txt_filepath, bin_filepath in zip(args.txt_files, args.bin_files):
        convert_txt_to_bin(txt_filepath, bin_filepath)
        print(f"Converted {txt_filepath} to {bin_filepath}")