import argparse

parser = argparse.ArgumentParser(
    prog="addcol",
    description="Takes an optional directory prefix and a file,"
    " and combines all the input files that are found"
    " into single outputfile",
)
parser.add_argument("--inputDirPrefix")
parser.add_argument("--inputFile", required=True)
parser.add_argument("-o", "--outputFile", required=True)
args = parser.parse_args()


with open(args.outputFile, "wt", encoding="utf8") as ofile:
    with open(args.inputFile, "rt", encoding="utf8") as ifile:
        ofile.write(ifile.read())
