import sqlite3

conn = sqlite3.connect("robot.db")
cursor = conn.cursor()


"""Task 3.1:"""

print("Task 3.1: ")
statement = '''
SELECT r.name, MAX(t.x) AS max_x, MIN(t.x) AS min_x
FROM Robot r JOIN Trajectory t ON r.robot_id = t.robot_id
GROUP BY r.name
ORDER BY r.robot_id;
'''

res = cursor.execute(statement)

relational_schema = [x[0] for x in res.description]

print(relational_schema)
print('------------')
for x in res:
    print(x)

print("\n")

"""Task 3.2:"""

print("Task 3.2:")
statement = '''
SELECT r.name, MAX(t.y) AS max_y, MIN(t.y) AS min_y
FROM Robot r JOIN Trajectory t ON r.robot_id = t.robot_id
GROUP BY r.name
ORDER BY r.robot_id;
'''

res = cursor.execute(statement)

relational_schema = [x[0] for x in res.description]

print(relational_schema)
print('------------')
for x in res:
    print(x)

print("\n")

"""Task 4.1:"""

print("Task 4.1: ")

statement = '''
SELECT DISTINCT t1.timestamp, MIN(MIN(t1.x, t2.x)) as x_min, MAX(MAX(t1.x, t2.x)) as x_max,
MIN(MIN(t1.y, t2.y)) as y_min, MAX(MAX(t1.y, t2.y)) as y_max
FROM Trajectory t1
JOIN Robot r1 ON t1.robot_id = r1.robot_id
JOIN Trajectory t2 ON t1.timestamp = t2.timestamp
JOIN Robot r2 ON t2.robot_id = r2.robot_id
WHERE r1.name = 'Astro' AND r2.name = 'IamHuman'
  AND ABS(t1.x - t2.x) < 1
  AND ABS(t1.y - t2.y) < 1
GROUP BY t1.timestamp;
'''

res = cursor.execute(statement)

relational_schema = [x[0] for x in res.description]

print(relational_schema)
print('------------')
for x in res:
    print(x)

print("\n")

"""Task 4.2:"""

print("Task 4.2: ")

statement = '''
SELECT COUNT(DISTINCT t1.timestamp) as close_seconds
FROM Trajectory t1
JOIN Robot r1 ON t1.robot_id = r1.robot_id
JOIN Trajectory t2 ON t1.timestamp = t2.timestamp
JOIN Robot r2 ON t2.robot_id = r2.robot_id
WHERE r1.name = 'Astro' AND r2.name = 'IamHuman'
  AND ABS(t1.x - t2.x) < 1
  AND ABS(t1.y - t2.y) < 1
'''

res = cursor.execute(statement)

relational_schema = [x[0] for x in res.description]

print(relational_schema)
print('------------')
for x in res:
    print(x)

print("\n")

"""Bonus:"""

print("Bonus: ")

statement = '''
SELECT interval_id, event_type FROM Interval main_it
WHERE EXISTS
	(SELECT * FROM
		(SELECT robot_id, SUM(SQRT(POWER(x2 - x1, 2) + POWER(y2 - y1, 2))) / ((SELECT end_time -1 FROM Interval WHERE interval_id = main_it.interval_id) - (SELECT start_time FROM Interval WHERE interval_id = main_it.interval_id)) as avg_speed
		FROM
			(SELECT t1.robot_id,
			t1.timestamp,
			t1.x AS x1, t1.y AS y1,
			t2.x AS x2, t2.y AS y2
			FROM Trajectory t1
			JOIN Trajectory t2
			WHERE t1.robot_id = t2.robot_id
			AND t2.timestamp = t1.timestamp + 1
			AND t1.timestamp BETWEEN
			(SELECT start_time FROM Interval WHERE interval_id = main_it.interval_id) AND (SELECT end_time -1 FROM Interval WHERE interval_id = main_it.interval_id))
		GROUP BY robot_id)
	WHERE avg_speed < .2)
'''
res = cursor.execute(statement)

rows = res.fetchall()

statement2 = '''
SELECT interval_id FROM Interval
'''
res2 = cursor.execute(statement2)

print(["interval_id", "answer"])
print('------------')
for x in res2:  #checks to see if there was a robot in which average speed was less than .2 cm/s. If the id is present returned by query, then we know its true; otherwise false. 
	present = False
	for y in rows:
		if (y[0] == x[0]):
			present = True
			x = f"({x[0]}, Yes)"
			continue
	if (not present):
		x = f"({x[0]}, No)"
	print(x)
      

conn.commit()
conn.close()