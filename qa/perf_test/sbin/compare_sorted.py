import sys
import os
import itertools  # import itertools module

# ANSI color codes for red, green, blue text
RED = "\033[91m"
GREEN = "\033[92m"
BLUE = "\033[94m"
RESET = "\033[0m"  # Reset color

def compare_csv(actual_file, expected_file, diff_file_path, tolerance=0.01, debug=False):
    differences = []  # Used for storing differential information
    with open(actual_file, 'r') as f_actual, open(expected_file, 'r') as f_expected:
        actual_header = f_actual.readline().strip().split(',')
        expected_header = f_expected.readline().strip().split(',')

        # Make sure the file headers are consistent
        if actual_header != expected_header:
            print(f"{RED}Header mismatch!{RESET}")
            print(f"Actual: {RED}{actual_header}{RESET}")
            print(f"Expected: {GREEN}{expected_header}{RESET}\n")
            # Remove the color code in differences
            differences.append(f"Header mismatch!\nActual: {actual_header}\nExpected: {expected_header}\n")
            return False

        all_match = True
        row_number = 1

        # Use itertools.zip_longest to read and compare file contents line by line
        for actual_line, expected_line in itertools.zip_longest(f_actual, f_expected):
            row_number += 1

            # Handle situations where the actual file is shorter than the expected file
            if actual_line is None:
                print(f"{RED}Actual file ended prematurely at line {row_number}{RESET}")
                print(f"Expected line: {GREEN}{expected_line.strip().split(',')}{RESET}")
                differences.append(f"Actual file ended prematurely at line {row_number}\nExpected line:\n< {expected_line.strip().split(',')}\n")
                all_match = False
                continue

            # Handle situations where the expected file is shorter than the actual file
            if expected_line is None:
                print(f"{RED}Expected file ended prematurely at line {row_number}{RESET}")
                print(f"Actual line: {RED}{actual_line.strip().split(',')}{RESET}")
                differences.append(f"Expected file ended prematurely at line {row_number}\nActual line:\n> {actual_line.strip().split(',')}\n")
                all_match = False
                continue

            actual_row = actual_line.strip().split(',')
            expected_row = expected_line.strip().split(',')

            # Skip blank lines or lines starting with "Time"
            if not actual_row or actual_row[0].startswith('Time'):
                continue

            row_match = True
            for actual_val, expected_val in zip(actual_row, expected_row):
                # Try to convert the value to a number and make a fuzzy comparison
                try:
                    actual_num = float(actual_val)
                    expected_num = float(expected_val)
                    diff = abs(actual_num - expected_num)
                    max_value = max(abs(actual_num), abs(expected_num))
                    if max_value == 0:
                        relative_diff = 0
                    else:
                        relative_diff = diff / max_value
                    if relative_diff > tolerance:
                        row_match = False
                        break  # If the value exceeds the tolerance, exit the comparison directly
                except ValueError:
                    # Strict comparison of strings
                    if actual_val != expected_val:
                        row_match = False
                        break  # If the strings do not match, exit the comparison directly

            if row_match and debug:
                # In debug mode, the matching lines are marked in blue
                print(f"Row {row_number} {BLUE}Same:{RESET}")
                print(f"Actual:\n> {BLUE}{actual_row}{RESET}")
                print(f"Expected:\n< {BLUE}{expected_row}{RESET}")
            elif not row_match:
                # When there is a mismatch, print and record the difference information
                print(f"Row {row_number} difference:")
                print(f"Actual:\n> {RED}{actual_row}{RESET}")
                print(f"Expected:\n< {GREEN}{expected_row}{RESET}")
                print("-" * 50)
                # Remove the color code in differences
                differences.append(f"Row {row_number} difference:\nActual:\n> {actual_row}\nExpected:\n< {expected_row}\n{'-' * 50}\n")
                all_match = False

        # If there are any differences, write them into the diff file
        if differences:
            with open(diff_file_path, 'w') as diff_file:
                diff_file.writelines(differences)

        return all_match  # Return the matching result

if __name__ == "__main__":
    if len(sys.argv) < 4:
        print("Usage: python3 compare_sorted.py <expected_dir> <actual_dir> <csv_names> [--debug]")
        sys.exit(1)

    expected_dir = sys.argv[1]
    actual_dir = sys.argv[2]
    csv_names = sys.argv[3].split(',')  # A comma-separated list of CSV file names
    debug = '--debug' in sys.argv  # Check whether the debug mode is enabled

    # Initialize the matching flag and the list of files that have not passed the comparison
    all_match = True
    mismatched_files = []  # Used to record CSV file names that have not passed the comparison

    # Traverse each CSV file name and make comparisons
    for csv_name in csv_names:
        expected_file = os.path.join(expected_dir, f"{csv_name}.csv")
        actual_file = os.path.join(actual_dir, f"{csv_name}.csv")
        diff_file_path = os.path.join(actual_dir, f"{csv_name}.diff")

        print(f"Comparing {csv_name}.csv...")
        result = compare_csv(actual_file, expected_file, diff_file_path, debug=debug)
        if not result:
            all_match = False
            mismatched_files.append(csv_name)  # Record the file names that failed the comparison
            # When a mismatch is detected, print the current query name
            print(f"{RED}Mismatch found in query: {csv_name}{RESET}")
            print(f"Differences have been written to {diff_file_path}")
        print("=" * 50)

    if all_match:
        print(f"All {len(csv_names)} CSV files match!")
        sys.exit(0)  # All files match, and the exit status is 0
    else:
        mismatched_count = len(mismatched_files)
        total_files = len(csv_names)
        print(f"{RED}{mismatched_count} out of {total_files} CSV files do not match!{RESET}")
        print("Mismatched files:")
        for fname in mismatched_files:
            print(f"- {fname}.csv")
        sys.exit(1)  # There is a file mismatch and the exit status is 1