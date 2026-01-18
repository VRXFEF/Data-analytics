import psycopg2
import time
import random

conn = psycopg2.connect(host="db", database="analytics_db", user="user", password="password")
cur = conn.cursor()
cur.execute("CREATE TABLE IF NOT EXISTS weather (id SERIAL, dt TIMESTAMP DEFAULT CURRENT_TIMESTAMP, temperature FLOAT);")
conn.commit()

while True:
    temp = round(random.uniform(20, 30), 2)
    cur.execute("INSERT INTO weather (temperature) VALUES (%s)", (temp,))
    conn.commit()
    print(f"Записана температура: {temp}")
    time.sleep(5)