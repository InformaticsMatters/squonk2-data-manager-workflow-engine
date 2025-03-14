import argparse

parser = argparse.ArgumentParser(
    prog="shortcut-example-1-process-a",
    description="The Job for the first step in our Shortcut example 1",
)
parser.add_argument("-i", "--inputFile", required=True)
parser.add_argument("-o", "--outputFile", required=True)
args = parser.parse_args()

print(args)

with open(args.inputFile, "rt", encoding="utf8") as input_file:
    content = input_file.read()
    with open(args.outputFile, "wt", encoding="utf8") as output_file:
        output_file.write(content)
