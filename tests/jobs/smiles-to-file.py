import argparse

parser = argparse.ArgumentParser(
    prog="smiles-to-file",
    description="Takes an input SMILES string and writes it to a file",
)
parser.add_argument("-s", "--smiles", required=True)
parser.add_argument("-o", "--outputFile", required=True)
args = parser.parse_args()

with open(args.outputFile, "wt", encoding="utf8") as output_file:
    output_file.write(f"{args.smiles}\n")
