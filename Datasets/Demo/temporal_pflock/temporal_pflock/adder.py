
epsilon = 0.5
factor = 1000
with open('output.tsv', 'r') as reader:
    # Read and print the entire file line by line
    for line in reader:
        arr = line.split("\t")
        x = int(float(arr[1]) * factor)
        y = int(float(arr[2]) * factor)
        print(f"{arr[0]}1\t{x + epsilon}\t{y}\t{arr[3]}", end = '')
        print(f"{arr[0]}2\t{x}\t{y}\t{arr[3]}", end = '')
        print(f"{arr[0]}3\t{x}\t{y + epsilon}\t{arr[3]}", end = '')
