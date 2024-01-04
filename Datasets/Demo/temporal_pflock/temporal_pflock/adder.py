
epsilon = 0.005
with open('output.tsv', 'r') as reader:
    # Read and print the entire file line by line
    for line in reader:
        arr = line.split("\t")
        x = float(arr[1])
        y = float(arr[2])
        print(f"{arr[0]}1\t{x + epsilon}\t{y}\t{arr[3]}", end = '')
        print(f"{arr[0]}2\t{x}\t{y}\t{arr[3]}", end = '')
        print(f"{arr[0]}3\t{x}\t{y + epsilon}\t{arr[3]}", end = '')
