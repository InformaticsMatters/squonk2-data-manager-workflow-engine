import argparse

parser = argparse.ArgumentParser(
    prog="addcol",
    description="Takes a list of files and writes them into single outputfile",
)
parser.add_argument("inputFile", nargs="+", type=argparse.FileType("r"))
parser.add_argument("-o", "--outputFile", required=True)
args = parser.parse_args()


with open(args.outputFile, "wt", encoding="utf8") as ofile:
    for f in args.inputFile:
        ofile.write(f.read())
