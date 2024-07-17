import subprocess
import time

import psycopg2
import os
import glob
import numpy as np
import matplotlib.pyplot as plt
import random


def start_postgres():
    try:
        # Start the PostgreSQL server on WSL
        subprocess.run(["wsl", "/usr/local/pgsql/bin/pg_ctl", "-D", "/usr/local/pgsql/data", "-l", "logfile", "start"],
                       check=True)
        print("PostgreSQL server started on WSL.")

        # Optionally, you can wait for a few seconds to ensure the server has started
        time.sleep(5)
    except subprocess.CalledProcessError as e:
        print(f"Failed to start PostgreSQL server: {e}")


def stop_postgres():
    try:
        # Stop the PostgreSQL server on WSL
        subprocess.run(["wsl", "/usr/local/pgsql/bin/pg_ctl", "-D", "/usr/local/pgsql/data", "stop"], check=True)
        print("PostgreSQL server stopped on WSL.")
    except subprocess.CalledProcessError as e:
        print(f"Failed to stop PostgreSQL server: {e}")


def connect_to_db(db_params, sql_files, mode):
    total_execution_time = 0
    start_postgres()
    print("Running in " + mode)
    psql_exec_time = []
    psql_hits = []
    psql_reads = []
    psql_query_names = []

    connection = None
    try:
        # Connect to the database
        connection = psycopg2.connect(**db_params)
        cursor = connection.cursor()
        cursor.execute("LOAD 'pg_hot_cold_start';")

        if mode == "hot":
            cursor.execute("SET experiment_mode = 'hot';")
        elif mode == "cold":
            cursor.execute("SET experiment_mode = 'cold';")
        else:
            cursor.execute("SET experiment_mode = 'off';")

        for file in sql_files:
            psql_query_names.append(os.path.basename(file))
            query = read_sql_content(file)
            cursor.execute("EXPLAIN (ANALYZE, BUFFERS) " + query)
            output = cursor.fetchall()
            if output:
                execution_time = extract_execution_time(output[-1][0])
                try:
                    shared_hits = str(output[1][0]).split(" ")[4].split("=")[1]
                except IndexError:
                    shared_hits = "0"  # if index is out of range

                # Handle shared reads
                try:
                    shared_reads = str(output[1][0]).split(" ")[5].split("=")[1]
                except IndexError:
                    shared_reads = "0"  # if index is out of range

                if execution_time is not None:
                    psql_exec_time.append(execution_time)
                    total_execution_time = total_execution_time + execution_time
                    print(total_execution_time)
                    psql_hits.append(shared_hits)
                    psql_reads.append(shared_reads)

        # Close the cursor and connection
        cursor.close()

    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error occurred: {error}")

    finally:
        if connection is not None:
            connection.close()
            print("Database connection closed.")
            stop_postgres()

    return psql_query_names, psql_exec_time, psql_hits, psql_reads, total_execution_time


def read_sql_content(file):
    with open(file, "r") as sql_file:
        content = sql_file.read()
    return content


def extract_execution_time(output):
    try:
        parts = output.split(":")
        if len(parts) > 1:
            time_part = parts[1].strip().split(" ")[0]
            return float(time_part)
    except Exception as e:
        print(f"Failed to extract execution time: {e}")
    return None


def visualize_execution_time(query_names, execution_times_cold, hits_cold, reads_cold,
                             execution_times_hot, hits_hot, reads_hot,
                             execution_times_off, hits_off, reads_off,
                             total_execution_time_off, total_execution_time_cold, total_execution_time_hot
                             ):
    # Number of datasets
    n = 3

    # Width of each bar
    bar_width = 0.2

    # Create an array of bar positions
    indices = np.arange(len(query_names))

    # Create figure and axes
    fig, ax1 = plt.subplots(1, 1, figsize=(18, 12))

    # Plot 1: Execution Time with offsets
    ax1.bar(indices - bar_width, execution_times_cold, bar_width, color='skyblue', edgecolor='black', label='Cold')
    ax1.bar(indices, execution_times_hot, bar_width, color='salmon', edgecolor='black', label='Hot')
    ax1.bar(indices + bar_width, execution_times_off, bar_width, color='lightgreen', edgecolor='black', label='Off')
    ax1.set_title('Execution Time')
    ax1.set_ylabel('Time (ms)')
    ax1.grid(axis='y', linestyle='--', alpha=0.7)
    ax1.set_xticks(indices)
    ax1.set_xticklabels(query_names)
    ax1.tick_params(axis='x', rotation=90, labelsize=8)  # Rotate x labels and adjust fontsize
    ax1.set_ylim(0, max(execution_times_cold) + 500)
    ax1.text(10, 1625, "Cold Start:" + str(total_execution_time_cold) + "ms", style='italic')
    ax1.text(10, 1825, "Hot Start:" + str(total_execution_time_hot) + "ms", style='italic')
    ax1.text(10, 2025, "No Extension:" + str(total_execution_time_off) + "ms", style='italic')

    # Annotate execution time bars
    # for i in range(len(query_names)):
    #    ax1.text(i - bar_width, execution_times_cold[i] + 50, str(round(execution_times_cold[i], 2)), ha='center',
    #             va='bottom', fontsize=8)
    #    ax1.text(i, execution_times_hot[i] + 50, str(round(execution_times_hot[i], 2)), ha='center', va='bottom',
    #             fontsize=8)
    #    ax1.text(i + bar_width, execution_times_off[i] + 50, str(round(execution_times_off[i], 2)), ha='center',
    #             va='bottom', fontsize=8)

    # Add legend
    ax1.legend()

    # Adjust layout
    plt.tight_layout()

    # Display the plots
    plt.show()


def visualize_data_buffer(query_names, execution_times_cold, hits_cold, reads_cold,
                          execution_times_hot, hits_hot, reads_hot,
                          execution_times_off, hits_off, reads_off,
                          total_execution_time_off, total_execution_time_cold, total_execution_time_hot
                          ):
    # Number of datasets
    n = 3

    # Width of each bar
    bar_width = 0.2

    # Create an array of bar positions
    indices = np.arange(len(query_names))

    # Create figure and axes
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(18, 12))

    # Plot 1: Execution Time with offsets
    ax1.bar(indices - bar_width, hits_cold, bar_width, color='skyblue', edgecolor='black', label='Cold')
    ax1.bar(indices, hits_hot, bar_width, color='salmon', edgecolor='black', label='Hot')
    ax1.bar(indices + bar_width, hits_off, bar_width, color='lightgreen', edgecolor='black', label='Off')
    ax1.set_title('Shared Buffer Hits')
    ax1.set_ylabel('Hits')
    ax1.grid(axis='y', linestyle='--', alpha=0.7)
    ax1.set_xticks(indices)
    ax1.set_xticklabels(query_names)
    ax1.tick_params(axis='x', rotation=90, labelsize=8)  # Rotate x labels and adjust fontsize

    # Plot 2: Execution Time with offsets
    ax2.bar(indices - bar_width, reads_cold, bar_width, color='skyblue', edgecolor='black', label='Cold')
    ax2.bar(indices, reads_hot, bar_width, color='salmon', edgecolor='black', label='Hot')
    ax2.bar(indices + bar_width, reads_off, bar_width, color='lightgreen', edgecolor='black', label='Off')
    ax2.set_title('Reads')
    ax2.set_ylabel('Reads')
    ax2.grid(axis='y', linestyle='--', alpha=0.7)
    ax2.set_xticks(indices)
    ax2.set_xticklabels(query_names)
    ax2.tick_params(axis='x', rotation=90, labelsize=8)  # Rotate x labels and adjust fontsize

    # Annotate execution time bars
    # for i in range(len(query_names)):
    #    ax1.text(i - bar_width, execution_times_cold[i] + 50, str(round(execution_times_cold[i], 2)), ha='center',
    #             va='bottom', fontsize=8)
    #    ax1.text(i, execution_times_hot[i] + 50, str(round(execution_times_hot[i], 2)), ha='center', va='bottom',
    #             fontsize=8)
    #    ax1.text(i + bar_width, execution_times_off[i] + 50, str(round(execution_times_off[i], 2)), ha='center',
    #             va='bottom', fontsize=8)

    # Add legend
    ax1.legend()

    # Adjust layout
    plt.tight_layout()

    # Display the plots
    plt.show()


def main():
    path = "../res/queries/test/"

    stop_postgres()
    #start_postgres()

    # Database connection parameters
    db_params = {
        'dbname': 'imdb',
        'user': 'jan',
        'password': '',
        'host': 'localhost',  # e.g., 'localhost'
        'port': '5432'  # e.g., '5432'
    }

    # Get files from the directory
    sql_files = sorted(glob.glob(os.path.join(path, '*')), key=len)

    # random.seed(18)
    # random.shuffle(sql_files)

    # Cold Start

    query_names, execution_times_off, hits_off, reads_off, total_execution_time_off = connect_to_db(db_params,
                                                                                                    sql_files,
                                                                                                    "off")
    query_names, execution_times_cold, hits_cold, reads_cold, total_execution_time_cold = connect_to_db(db_params,
                                                                                                        sql_files,
                                                                                                        "cold")
    query_names, execution_times_hot, hits_hot, reads_hot, total_execution_time_hot = connect_to_db(db_params,
                                                                                                    sql_files,
                                                                                                    "hot")

    # Visualize the data
    visualize_execution_time(query_names, execution_times_cold, hits_cold, reads_cold,
                             execution_times_hot, hits_hot, reads_hot,
                             execution_times_off, hits_off, reads_off,
                             total_execution_time_off, total_execution_time_cold, total_execution_time_hot)

    visualize_data_buffer(query_names, execution_times_cold, hits_cold, reads_cold,
                          execution_times_hot, hits_hot, reads_hot,
                          execution_times_off, hits_off, reads_off,
                          total_execution_time_off, total_execution_time_cold, total_execution_time_hot)


if __name__ == "__main__":
    main()
