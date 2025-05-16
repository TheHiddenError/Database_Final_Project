import pandas as pd
import sqlite3

conn = sqlite3.connect("robot.db")
cursor = conn.cursor()

cursor.execute("""
CREATE TABLE Robot (
    robot_id INT PRIMARY KEY,
    name TEXT
);
""")

df = pd.read_csv("data_files/robot.csv", header=None, names=["robot_id", "name"])

for index, row in df.iterrows():
  cursor.execute("INSERT INTO Robot (robot_id, name) VALUES (?, ?)",
  (row["robot_id"], row["name"]))


cursor.execute("""
CREATE TABLE Trajectory (
  robot_id INTEGER,
  timestamp INTEGER,
  x REAL,
  y REAL,
  PRIMARY KEY (robot_id, timestamp)
  FOREIGN KEY (robot_id) REFERENCES Robot(robot_id)
)
""")

for i in range(1, 6):  # t1.csv to t5.csv
    robot_id = i
    file = f"data_files/t{i}.csv"
    df = pd.read_csv(file, header=None, names=["x", "y"])

    for index, row in df.iterrows():
        timestamp = index + 1
        cursor.execute(
            "INSERT INTO Trajectory (robot_id, timestamp, x, y) VALUES (?, ?, ?, ?)",
            (robot_id, timestamp, row["x"], row["y"])
        )

cursor.execute("""
CREATE TABLE Interval (
  interval_id INTEGER PRIMARY KEY AUTOINCREMENT,
  start_time INTEGER,
  end_time INTEGER,
  event_type TEXT
)
""")

df = pd.read_csv("data_files/interval.csv", header=None, names=["start_time", "end_time", "event_type"])

for _, row in df.iterrows():
  cursor.execute("INSERT INTO Interval (start_time, end_time, event_type) VALUES (?, ?, ?)",
  (row["start_time"], row["end_time"], row["event_type"]))

conn.commit()
conn.close()