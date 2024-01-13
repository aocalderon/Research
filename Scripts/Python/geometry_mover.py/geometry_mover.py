import csv

def displace(data, offset_x, offset_y):
    temp = []
    for row in data:
        i = row[0]
        x = row[1] + offset_x
        y = row[2] + offset_y
        t = row[3]
        temp.append([i, x, y, t])
    return temp
        
def reader_and_displace(file, offset_x, offset_y):
    data = []
    with open(file, "r") as f:
        reader = csv.reader(f)
        for i, line in enumerate(reader):
            i = line[0]
            x = float(line[1]) + offset_x
            y = float(line[2]) + offset_y
            t = line[3]
            data.append(f"{i},{x},{y},{t}")
        
    for row in data:
        print(f"{row}")

def read(file):
    data = []
    with open(file, "r") as f:
        reader = csv.reader(f)
        for i, line in enumerate(reader):
            i = int(line[0])
            x = float(line[1])
            y = float(line[2])
            t = int(line[3])
            data.append([i, x, y, t])
    return data

def split(data, offset_x, offset_y, split_x, split_y):
    nw = []
    ne = []
    sw = []
    se = []
    for row in data:
        if(row[1] < split_x and row[2] < split_y):
            se.append(row)
        elif(row[1] < split_x and row[2] >= split_y):
            ne.append(row)
        elif(row[1] >= split_x and row[2] < split_y):
            sw.append(row)
        else:
            nw.append(row)
    new_sw = displace(sw, offset_x, 0)
    new_ne = displace(ne,  0, offset_y)
    new_nw = displace(nw, offset_x, offset_y)
    new_data = se + new_sw + new_ne + new_nw

    return new_data

def main(file, offset_x, offset_y, split_x, split_y):
    data = read(file)
    result = split(data, offset_x, offset_y, split_x, split_y)
    for row in result:
        print(f"{row[0]}\t{row[1]}\t{row[2]}\t{row[3]}")
        
if __name__ == "__main__":

    import argparse

    parser = argparse.ArgumentParser(description="Geometry mover.")
    parser.add_argument("input", metavar="INPUT", help="Input file name.")
    parser.add_argument("--offx", type = int, default=10, help="Offset in X.")
    parser.add_argument("--offy", type = int, default=10, help="Offset in Y.")
    parser.add_argument("--splx", type = int, default=50, help="Split at X.")
    parser.add_argument("--sply", type = int, default=50, help="Split at Y.")
    args = parser.parse_args()

    main(args.input, args.offx, args.offy, args.splx, args.sply)
