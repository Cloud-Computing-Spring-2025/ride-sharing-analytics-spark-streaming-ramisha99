# Real-Time Ride-Sharing Analytics with Apache Spark

This project simulates a ride-sharing platform and processes real-time trip data using **Apache Spark Structured Streaming**. The pipeline includes data ingestion, driver-level aggregations, and time-windowed trend analysis.

---

## Project Structure

├── task1_ingest_parse.py # Task 1: Ingest & parse JSON ride data 


├── task2_driver_aggregations.py # Task 2: Driver-level fare & distance analysis 


├── task3_time_window_analysis.py # Task 3: Time-windowed fare trend analysis 


├── simulator.py # Simulates real-time ride data on localhost:9999 


├── output/ # Contains CSV results from each task 


└── checkpoints/ # Spark streaming checkpoint folders

---

## Setup Instructions

###  Install Requirements

```
pip install pyspark faker
sudo apt install default-jdk -y  # Required for PySpark
2. Start the Ride Data Simulator

python3 data_generator.py

```
This will stream fake ride events to localhost:9999.
```
###Task 1: Ingest & Parse Ride Data
File: task1_ingest_parse.py
What it does:
Connects to the socket on localhost:9999

Parses incoming JSON messages

Writes parsed output to CSV

Run the task:

python3 task1_ingest_parse.py
Sample Output (CSV):

output/task1_parsed_rides/part-*.csv

trip_id,driver_id,distance_km,fare_amount,timestamp
bd81f2d1-ae1d-4d1b-9d42-3dcac7c1590f,21,12.5,45.75,2025-04-01 17:43:20

###Task 2: Driver-Level Aggregations
File: task2_driver_aggregations.py
What it does:
Groups data by driver_id and 1-minute time windows

Calculates total fare and average distance

Writes output to CSV

Run the task:

python3 task2_driver_aggregations.py
Sample Output (CSV):

output/task2_driver_aggregates/part-*.csv

driver_id,total_fare,avg_distance,window_start,window_end
32,153.20,12.3,2025-04-01 17:42:00,2025-04-01 17:43:00

###Task 3: Time-Windowed Fare Trends
File: task3_time_window_analysis.py

What it does:
Applies 1-minute sliding windows to all trip events

Aggregates total fare per window

Writes time-based trends to CSV

Run the task:

python3 task3_time_window_analysis.py
Sample Output (CSV):

output/task3_windowed_fares/part-*.csv

total_fare,window_start,window_end
261.70,2025-04-01 17:42:00,2025-04-01 17:43:00
```
Notes
- CSVs update in real time in the output/ directory

- Checkpoints are automatically managed in the checkpoints/ folder

- Make sure the simulator is running before launching any task

- For faster testing, Task 3 uses a short time window (1 minute with 30 sec slides)

-Run All Tasks Independently

In different terminals (after running the simulator):
```
python3 task1_ingest_parse.py
python3 task2_driver_aggregations.py
python3 task3_time_window_analysis.py
