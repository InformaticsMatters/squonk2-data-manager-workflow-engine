import shutil
import sys
from pathlib import Path


def main():
    print("copyf job runnint")
    if len(sys.argv) != 2:
        print("Usage: python copy_file.py <filename>")
        sys.exit(1)

    original_path = Path(sys.argv[1])

    if not original_path.exists() or not original_path.is_file():
        print(f"Error: '{original_path}' does not exist or is not a file.")
        sys.exit(1)

    # Create a new filename like 'example_copy.txt'
    new_name = original_path.absolute().parent.joinpath("chunk_1.smi")
    new_path = original_path.with_name(new_name.name)
    shutil.copyfile(original_path, new_path)

    new_name = original_path.absolute().parent.joinpath("chunk_2.smi")
    new_path = original_path.with_name(new_name.name)

    shutil.copyfile(original_path, new_path)


if __name__ == "__main__":
    main()
