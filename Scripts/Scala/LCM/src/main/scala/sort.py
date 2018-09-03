f = open("lcm.log", "r")
for line in f:
    print(" ".join(sorted(line.replace("\n","").split(" "))))

