import csv
import os
from collections import defaultdict


def find_row_count_discrepancies(file_list: list[str]):
    """
    Reads multiple time-series CSV files and identifies timestamps where the number
    of data points (rows) is not consistent across all files.

    Args:
        file_list: A list of CSV file paths to compare.
    """
    if not file_list:
        print("Error: The file list is empty. Nothing to compare.")
        return


    timestamp_counts = defaultdict(lambda: defaultdict(int))


    print("--- Reading and processing files... ---")
    for filename in file_list:
        if not os.path.exists(filename):
            print(f"Warning: File not found, skipping: {filename}")
            continue

        print(f"Reading {filename}...")
        with open(filename, 'r', newline='') as f:
            reader = csv.reader(f)
            try:
                # Skip the header row
                next(reader)
                for row in reader:
                    if not row: continue  # Skip empty rows
                    timestamp_hour = row[0][:10]  # Key is the full timestamp YYYYMMDDHH
                    timestamp_counts[timestamp_hour][filename] += 1
            except StopIteration:
                print(f"Warning: File is empty (or only has a header): {filename}")
            except IndexError:
                print(f"Warning: Found a malformed row in {filename}. Please check the file.")

    if not timestamp_counts:
        print("No data found in any of the files. Comparison cannot be performed.")
        return

    # Iterate through all timestamps and compare
    print("\n--- Comparing data... Looking for discrepancies... ---")

    discrepancy_found = False
    # Sort the timestamps chronologically
    for timestamp in sorted(timestamp_counts.keys()):
        # Get the counts for the current timestamp from all files.
        # Use .get(file, 0) for safety :)
        counts_for_this_timestamp = [timestamp_counts[timestamp].get(f, 0) for f in file_list]

        # Check for discrepancies
        if len(set(counts_for_this_timestamp)) > 1:
            discrepancy_found = True
            print(f"\nDiscrepancy found at Timestamp (Hour): {timestamp}")

            # Print report for timestamp
            for i, filename in enumerate(file_list):
                count = counts_for_this_timestamp[i]
                print(f"  - {filename:<45}: {count} entries")

    # Final Summary
    if not discrepancy_found:
        print("\nComparison Complete: No discrepancies found. All files have consistent row counts for all timestamps.")
    else:
        print("\nComparison Complete: Found one or more discrepancies as listed above.")



if __name__ == "__main__":

    path = "C:\\Users\\jeppe\\Documents\\GitHub\\REO\\"
    files_to_compare = [
        path+"Horns_Rev_C_2025-01-01_to_2025-12-31.csv",
        path+"Horns_Rev_B_2025-01-01_to_2025-12-31.csv",
        path+"Horns_Rev_A_2025-01-01_to_2025-12-31.csv",
        path+"Vesterhav_Nord_2025-01-01_to_2025-12-31.csv",
        path+"Vesterhav_Syd_2025-01-01_to_2025-12-31.csv",
        path+"Solar_Park_Gedmosen_2025-01-01_to_2025-12-31.csv",
        path+"Solar_Park_Kassoe_2025-01-01_to_2025-12-31.csv",
        path+"Solar_Park_Holsted_2025-01-01_to_2025-12-31.csv"
    ]

    # 2. RUN THE SCRIPT :D
    find_row_count_discrepancies(files_to_compare)