#!/usr/bin/env python

import os
import sys
import random
import time
from concurrent.futures import ThreadPoolExecutor


def check_args(args):
    try:
        if len(args) != 2 or int(args[1]) <= 0:
            raise ValueError()
    except:
        print("Usage:  create_measurements.sh <positive integer number of records to create>")
        print("        You can use underscore notation for large number of records.")
        print("        For example:  1_000_000_000 for one billion")
        sys.exit(1)


def build_weather_station_name_list():
    station_names = set()
    with open('weather_stations.csv', 'r', encoding='utf-8') as file:
        for line in file:
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            station_name = line.split(';')[0].strip()
            station_names.add(station_name)
    return list(station_names)


def convert_bytes(num):
    for unit in ['bytes', 'KiB', 'MiB', 'GiB']:
        if num < 1024:
            return f"{num:.1f} {unit}"
        num /= 1024.0


def format_elapsed_time(seconds):
    if seconds < 60:
        return f"{seconds:.3f} seconds"
    elif seconds < 3600:
        minutes, seconds = divmod(seconds, 60)
        return f"{int(minutes)} minutes {int(seconds)} seconds"
    else:
        hours, remainder = divmod(seconds, 3600)
        minutes, seconds = divmod(remainder, 60)
        return f"{int(hours)} hours {int(minutes)} minutes {int(seconds)} seconds"


def estimate_file_size(station_names, num_rows):
    total_name_bytes = sum(len(s.encode("utf-8")) for s in station_names)
    avg_name_bytes = total_name_bytes / len(station_names)
    avg_temp_bytes = 4.400200100050025
    avg_line_length = avg_name_bytes + avg_temp_bytes + 2
    return f"Estimated max file size is:  {convert_bytes(num_rows * avg_line_length)}."


def generate_batch(station_pool, batch_size, coldest, hottest):
    return '\n'.join(
        f"{station};{random.uniform(coldest, hottest):.1f}"
        for station in random.choices(station_pool, k=batch_size)
    ) + '\n'


def build_test_data(station_names, num_rows):
    start_time = time.time()
    coldest, hottest = -99.9, 99.9
    batch_size = 100_000
    chunks = num_rows // batch_size
    station_pool = random.choices(station_names, k=10_000)

    print("Building test data...")

    try:
        with open("measurements.txt", 'w', encoding='utf-8', buffering=1024 * 1024) as file:
            with ThreadPoolExecutor() as executor:
                futures = [
                    executor.submit(generate_batch, station_pool, batch_size, coldest, hottest)
                    for _ in range(chunks)
                ]

                for i, future in enumerate(futures):
                    file.write(future.result())
                    progress = (i + 1) * 100 // chunks
                    if i == 0 or (i + 1) * 100 // chunks != i * 100 // chunks:
                        bars = '=' * (progress // 2)
                        sys.stdout.write(f"\r[{bars:<50}] {progress}%")
                        sys.stdout.flush()

        sys.stdout.write('\n')
    except Exception as e:
        print("Something went wrong. Printing error info and exiting...")
        print(e)
        sys.exit(1)

    elapsed = time.time() - start_time
    size = os.path.getsize("measurements.txt")

    print("Test data successfully written to 1brc/data/measurements.txt")
    print(f"Actual file size:  {convert_bytes(size)}")
    print(f"Elapsed time: {format_elapsed_time(elapsed)}")


def main():
    check_args(sys.argv)
    num_rows = int(sys.argv[1])
    station_names = build_weather_station_name_list()
    print(estimate_file_size(station_names, num_rows))
    build_test_data(station_names, num_rows)
    print("Test data build complete.")


if __name__ == "__main__":
    main()
