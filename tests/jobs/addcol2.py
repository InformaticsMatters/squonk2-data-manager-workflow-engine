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

with (
    open(args.inputFile, "rt", encoding="utf8") as ifile,
    open(args.outputFile, "wt", encoding="utf8") as ofile,
):
    for line in ifile.read().splitlines():
        ofile.write(f"{line}\t{args.value}\n")
