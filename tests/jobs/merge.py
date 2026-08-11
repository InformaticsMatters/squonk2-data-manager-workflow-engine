"""A test Job that merges two named input files into one output file.

Unlike 'concatenate' (whose input is a 'files' variable, which makes any step
using it a combiner) this Job takes two distinct, singular inputs. That lets a
workflow express a plain fan-in - one step that depends on two prior steps.
"""

import argparse

parser = argparse.ArgumentParser(
    prog="merge",
    description="Merges two input files into a single output file",
)
parser.add_argument("-a", "--inputFileA", required=True)
parser.add_argument("-b", "--inputFileB", required=True)
parser.add_argument("-o", "--outputFile", required=True)
args = parser.parse_args()

with open(args.outputFile, "wt", encoding="utf8") as ofile:
    for input_file in [args.inputFileA, args.inputFileB]:
        with open(input_file, "rt", encoding="utf8") as ifile:
            ofile.write(ifile.read())
