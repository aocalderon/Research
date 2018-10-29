import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--input_path",  "-i", required=True, help="Input file...")
parser.add_argument("--output_path", "-o", help="Output file...")
args = parser.parse_args()

output_unsort  = open(args.input_path,  'r')
output_sort    = open(args.output_path, 'w')
for pattern in output_unsort.readlines():
    p = " ".join(map(str, sorted([ int(x) for x in pattern.split(" ")])))
    output_sort.write(p + '\n')
output_unsort.close()
output_sort.close()
   
