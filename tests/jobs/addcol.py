import argparse

parser = argparse.ArgumentParser(
    prog="addcol",
    description="Takes a SMILES string and writes it to a file",
)
parser.add_argument("-i", "--inputFile", required=True)
parser.add_argument("-o", "--outputFile", required=True)
parser.add_argument("-n", "--name", required=True)
parser.add_argument("-v", "--value", required=True)
args = parser.parse_args()

# with open(args.inputFile, "rt", encoding="utf8") as input_file:
#     content = input_file.read()
#     with open(args.outputFile, "wt", encoding="utf8") as output_file:
#         output_file.write(content)
