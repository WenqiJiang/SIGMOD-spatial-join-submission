import argparse


class ObjT:
    def __init__(self, id, low0, high0, low1, high1):
        self.id = id
        self.low0 = low0
        self.high0 = high0
        self.low1 = low1
        self.high1 = high1


def read_file(filepath):
    objects = []

    with open(filepath, 'r') as file:
        # skip first line (count)
        next(file)

        for line in file:
            parts = line.split()

            if len(parts) == 5:
                id = int(parts[0])
                low0 = float(parts[1])
                high0 = float(parts[2])
                low1 = float(parts[3])
                high1 = float(parts[4])

                obj = ObjT(id, low0, high0, low1, high1)

                objects.append(obj)

    return objects


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Find intersections between objects in two files.")
    parser.add_argument('--filepathA', type=str, required=True, help="Path to the first file")
    parser.add_argument('--filepathB', type=str, required=True, help="Path to the second file")

    args = parser.parse_args()

    # read objs
    objectsA = read_file(args.filepathA)
    objectsB = read_file(args.filepathB)

    count = 0

    for objA in objectsA:
        for objB in objectsB:
            intersects = (objA.low0 < objB.high0 and objA.high0 > objB.low0) and (
                    objA.low1 < objB.high1 and objA.high1 > objB.low1)

            if intersects:
                count += 1

    print('Intersections: ' + str(count))