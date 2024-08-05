import matplotlib
matplotlib.use('Agg')  # Use the 'Agg' backend for non-interactive environments

import subprocess
import time
import psycopg2
import os
import glob
import numpy as np
import matplotlib.pyplot as plt
import random
import csv

# Determine the directory of the running script
script_dir = os.path.dirname(os.path.abspath(__file__))

def remove_log_file(log_file_path):
    try:
        if os.path.exists(log_file_path):
            subprocess.run(["sudo", "-u", "postgres", "rm", log_file_path], check=True)
    except subprocess.CalledProcessError as e:
        print(f"Failed to remove log file: {e}")

def start_postgres():
    log_file_path = "/home/ariskrasniqi/postgresql_logs/logfile"  # Use the new log directory
    try:
        # Check if PostgreSQL is already running
        print("Checking PostgreSQL server status...")
        result = subprocess.run(
            ["sudo", "-u", "postgres", "/usr/local/pgsql/bin/pg_ctl", "-D", "/usr/local/pgsql/data", "status"], 
            capture_output=True, text=True)
        if "server is running" in result.stdout:
            print("PostgreSQL server is already running.")
        else:
            print("Starting PostgreSQL server...")
            # Ensure the log file does not already exist
            remove_log_file(log_file_path)
            result = subprocess.run(
                ["sudo", "-u", "postgres", "/usr/local/pgsql/bin/pg_ctl", "-D", "/usr/local/pgsql/data", "-l", log_file_path, "start"], 
                capture_output=True, text=True)
            print("STDOUT:", result.stdout)
            print("STDERR:", result.stderr)
            result.check_returncode()
            print("PostgreSQL server started on WSL.")
            time.sleep(5)
    except subprocess.CalledProcessError as e:
        print(f"Failed to start PostgreSQL server: {e}")
        # Check the log file for errors
        if os.path.exists(log_file_path):
            with open(log_file_path, 'r') as file:
                log_contents = file.read()
                print("Log file contents:\n", log_contents)

def stop_postgres():
    try:
        print("Checking PostgreSQL server status...")
        result = subprocess.run(
            ["sudo", "-u", "postgres", "/usr/local/pgsql/bin/pg_ctl", "-D", "/usr/local/pgsql/data", "status"], 
            capture_output=True, text=True)
        if "no server running" in result.stdout:
            print("PostgreSQL server is already stopped.")
        else:
            print("Stopping PostgreSQL server...")
            result = subprocess.run(
                ["sudo", "-u", "postgres", "/usr/local/pgsql/bin/pg_ctl", "-D", "/usr/local/pgsql/data", "stop"], 
                capture_output=True, text=True)
            print("STDOUT:", result.stdout)
            print("STDERR:", result.stderr)
            result.check_returncode()
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
            
            execution_time = None
            total_shared_hits = 0
            total_shared_reads = 0
            
            is_scan = False
            for line in output:
                if any(scan in line[0] for scan in ["Seq Scan", "Index Scan", "Bitmap Heap Scan"]):
                    is_scan = True
                if "Execution Time" in line[0]:
                    execution_time = extract_execution_time(output)
                if is_scan and "Buffers:" in line[0]:
                    try:
                        parts = line[0].split(" ")
                        for part in parts:
                            if "hit=" in part:
                                total_shared_hits += int(part.split("=")[1])
                            if "read=" in part:
                                total_shared_reads += int(part.split("=")[1])
                        is_scan = False  # Reset after processing buffer info for scan
                    except Exception as e:
                        print(f"Error parsing buffers line: {e}")
                        is_scan = False  # Reset in case of error

            if execution_time is not None:
                psql_exec_time.append(execution_time)
                total_execution_time += execution_time
                print(f"Execution time for {file}: {execution_time} ms")
                print(f"Total execution time so far: {total_execution_time} ms")
                
            psql_hits.append(total_shared_hits)
            psql_reads.append(total_shared_reads)
            print(f"Total shared hits for {file}: {total_shared_hits}")
            print(f"Total shared reads for {file}: {total_shared_reads}")

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
        content = sql_file.read().strip()  # Remove any extraneous whitespace
    return content



def read_sql_content(file):
    with open(file, "r") as sql_file:
        content = sql_file.read()
    return content


def extract_execution_time(output):
    try:
        for line in output:
            if "Execution Time" in line[0]:
                parts = line[0].split(":")
                if len(parts) > 1:
                    time_part = parts[1].strip().split(" ")[0]
                    return float(time_part)
    except Exception as e:
        print(f"Failed to extract execution time: {e}")
    return None


def visualize_execution_time(query_names, execution_times_cold, hits_cold, reads_cold,
                             execution_times_hot,
                             execution_times_off,
                             total_execution_time_off, total_execution_time_cold, total_execution_time_hot):
    print(f"Query Names: {query_names}")  # Debug statement
    print(f"Execution Times Cold: {execution_times_cold}")  # Debug statement
    print(f"Execution Times Hot: {execution_times_hot}")  # Debug statement
    print(f"Execution Times Off: {execution_times_off}")  # Debug statement

    # Print lengths for debugging
    print(f"Length of query_names: {len(query_names)}")
    print(f"Length of execution_times_cold: {len(execution_times_cold)}")
    print(f"Length of execution_times_hot: {len(execution_times_hot)}")
    print(f"Length of execution_times_off: {len(execution_times_off)}")
    print(f"Length of hits_cold: {len(hits_cold)}")
    print(f"Length of reads_cold: {len(reads_cold)}")

    n = 3
    bar_width = 0.2
    indices = np.arange(len(query_names))
    fig, ax1 = plt.subplots(1, 1, figsize=(18, 12))
    ax1.bar(indices - bar_width, execution_times_cold, bar_width, color='skyblue', edgecolor='black', label='Cold')
    ax1.bar(indices, execution_times_hot, bar_width, color='salmon', edgecolor='black', label='Hot')
    ax1.bar(indices + bar_width, execution_times_off, bar_width, color='lightgreen', edgecolor='black', label='Off')
    ax1.set_title('Execution Time')
    ax1.set_ylabel('Time (ms)')
    ax1.grid(axis='y', linestyle='--', alpha=0.7)
    ax1.set_xticks(indices)
    ax1.set_xticklabels(query_names)
    ax1.tick_params(axis='x', rotation=90, labelsize=8)
    ax1.set_yscale('log')  # Set y-axis to logarithmic scale
    ax1.text(10, 10**3.5, "Cold Start:" + str(total_execution_time_cold) + "ms", style='italic')
    ax1.text(10, 10**3.75, "Hot Start:" + str(total_execution_time_hot) + "ms", style='italic')
    ax1.text(10, 10**4, "No Extension:" + str(total_execution_time_off) + "ms", style='italic')
    ax1.legend()
    plt.tight_layout()

    # Save the plot instead of showing it
    plt.savefig(os.path.join(script_dir, 'execution_time_plot_test.png'))
    plt.close()

def visualize_data_buffer(query_names, hits_cold, reads_cold,
                            hits_hot, reads_hot,
                            hits_off, reads_off):
    print(f"Query Names: {query_names}")  # Debug statement
    print(f"Hits Cold: {hits_cold}")  # Debug statement
    print(f"Reads Cold: {reads_cold}")  # Debug statement
    print(f"Hits Hot: {hits_hot}")  # Debug statement
    print(f"Reads Hot: {reads_hot}")  # Debug statement
    print(f"Hits Off: {hits_off}")  # Debug statement
    print(f"Reads Off: {reads_off}")  # Debug statement

    # Print lengths for debugging
    print(f"Length of query_names: {len(query_names)}")
    print(f"Length of hits_cold: {len(hits_cold)}")
    print(f"Length of reads_cold: {len(reads_cold)}")
    print(f"Length of hits_hot: {len(hits_hot)}")
    print(f"Length of reads_hot: {len(reads_hot)}")
    print(f"Length of hits_off: {len(hits_off)}")
    print(f"Length of reads_off: {len(reads_off)}")

    n = 3
    bar_width = 0.2
    indices = np.arange(len(query_names))
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(18, 12))
    ax1.bar(indices - bar_width, hits_cold, bar_width, color='skyblue', edgecolor='black', label='Cold')
    ax1.bar(indices, hits_hot, bar_width, color='salmon', edgecolor='black', label='Hot')
    ax1.bar(indices + bar_width, hits_off, bar_width, color='lightgreen', edgecolor='black', label='Off')
    ax1.set_title('Shared Buffer Hits')
    ax1.set_ylabel('Hits')
    ax1.grid(axis='y', linestyle='--', alpha=0.7)
    ax1.set_xticks(indices)
    ax1.set_xticklabels(query_names)
    ax1.tick_params(axis='x', rotation=90, labelsize=8)
    ax1.set_yscale('log')  # Set y-axis to logarithmic scale
    ax2.bar(indices - bar_width, reads_cold, bar_width, color='skyblue', edgecolor='black', label='Cold')
    ax2.bar(indices, reads_hot, bar_width, color='salmon', edgecolor='black', label='Hot')
    ax2.bar(indices + bar_width, reads_off, bar_width, color='lightgreen', edgecolor='black', label='Off')
    ax2.set_title('Reads')
    ax2.set_ylabel('Reads')
    ax2.grid(axis='y', linestyle='--', alpha=0.7)
    ax2.set_xticks(indices)
    ax2.set_xticklabels(query_names)
    ax2.tick_params(axis='x', rotation=90, labelsize=8)
    ax2.set_yscale('log')  # Set y-axis to logarithmic scale
    ax1.legend()
    plt.tight_layout()

    # Save the plot instead of showing it
    plt.savefig(os.path.join(script_dir, 'data_buffer_plot_test.png'))
    plt.close()

def visualize_execution_time_stacked(query_names, execution_times_cold, execution_times_hot, execution_times_off):
    n = len(query_names)
    indices = np.arange(n)
    bar_width = 0.5  # Slightly wider bars for better visibility

    fig, ax1 = plt.subplots(figsize=(18, 12))

    for i in range(n):
        times = {
            'cold': execution_times_cold[i],
            'hot': execution_times_hot[i],
            'off': execution_times_off[i]
        }
        sorted_times = sorted(times.items(), key=lambda x: x[1])

        base = 0
        for j, (label, value) in enumerate(sorted_times):
            color = 'skyblue' if label == 'cold' else 'salmon' if label == 'hot' else 'lightgreen'
            ax1.bar(indices[i], value - base, bar_width, bottom=base, color=color, edgecolor='black', label=label if i == 0 else "")
            base = value

    ax1.set_title('Execution Time')
    ax1.set_ylabel('Time (ms)')
    ax1.set_xticks(indices)
    ax1.set_xticklabels(query_names)
    ax1.tick_params(axis='x', rotation=90, labelsize=8)
    ax1.set_yscale('log')  # Set y-axis to logarithmic scale
    ax1.legend(loc='upper left')
    
    plt.tight_layout()
    plt.savefig(os.path.join(script_dir, 'execution_time_stacked_plot.png'))
    plt.close()

def visualize_data_buffer_stacked(query_names, hits_cold, reads_cold, hits_hot, reads_hot, hits_off, reads_off):
    n = len(query_names)
    indices = np.arange(n)
    bar_width = 0.5  # Slightly wider bars for better visibility

    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(18, 12))

    for ax, metric, plot_label in zip([ax1, ax2],
                                 [(hits_cold, hits_hot, hits_off), (reads_cold, reads_hot, reads_off)],
                                 ['Shared Buffer Hits', 'Reads']):
        for i in range(n):
            values = {
                'cold': metric[0][i],
                'hot': metric[1][i],
                'off': metric[2][i]
            }
            sorted_values = sorted(values.items(), key=lambda x: x[1])

            base = 0
            for j, (state, value) in enumerate(sorted_values):
                color = 'skyblue' if state == 'cold' else 'salmon' if state == 'hot' else 'lightgreen'
                ax.bar(indices[i], value - base, bar_width, bottom=base, color=color, edgecolor='black', label=state if i == 0 else "")
                base = value

        ax.set_title(plot_label)
        ax.set_ylabel(plot_label)
        ax.set_xticks(indices)
        ax.set_xticklabels(query_names)
        ax.tick_params(axis='x', rotation=90, labelsize=8)
        ax.set_yscale('log')  # Set y-axis to logarithmic scale
        ax.legend(loc='upper left')
    
    plt.tight_layout()
    plt.savefig(os.path.join(script_dir, 'data_buffer_stacked_plot.png'))
    plt.close()

    
def main():
    path = "../res/queries/test"

    stop_postgres()
    start_postgres()

    # Database connection parameters
    db_params = {
        'dbname': 'imdb',
        'user': 'postgres',
        'password': 'your_password',
        'host': 'localhost',
        'port': '5432'
    }
    
    # Get files from the directory
    sql_files = sorted(glob.glob(os.path.join(path, '*.sql')), key=os.path.basename)
    # sql_files = sorted(glob.glob(os.path.join(path, '*')), key=len)
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
    visualize_execution_time_stacked(query_names, execution_times_cold,
                             execution_times_hot,
                             execution_times_off)

    visualize_data_buffer_stacked(query_names, hits_cold, reads_cold,
                           hits_hot, reads_hot,
                           hits_off, reads_off)
    
    """visualize_execution_time(query_names, execution_times_cold, hits_cold, reads_cold,
                             execution_times_hot,
                             execution_times_off,
                             total_execution_time_off, total_execution_time_cold, total_execution_time_hot)
    
    visualize_data_buffer(query_names,hits_cold, reads_cold,
                          hits_hot, reads_hot,
                          hits_off, reads_off)"""



if __name__ == "__main__":
    main()
