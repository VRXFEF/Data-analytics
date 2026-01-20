import psycopg2
import time
import random

conn = psycopg2.connect(host="db", database="analytics_db", user="user", password="password")
cur = conn.cursor()

cur.execute("""
CREATE TABLE IF NOT EXISTS weather (
    id SERIAL PRIMARY KEY, 
    dt TIMESTAMP DEFAULT CURRENT_TIMESTAMP, 
    temperature FLOAT,
    humidity FLOAT,
    pressure FLOAT
);
""")
conn.commit()

print("Сервис запущен. Начинаю запись данных...")

while True:
  
    temp = round(random.uniform(20.0, 30.0), 2)      # Температура 20-30°C
    hum = round(random.uniform(40.0, 60.0), 2)       # Влажность 40-60%
    press = round(random.uniform(740.0, 760.0), 1)   # Давление 740-760 мм рт. ст.
    
    cur.execute(
        "INSERT INTO weather (temperature, humidity, pressure) VALUES (%s, %s, %s)", 
        (temp, hum, press)
    )
    conn.commit()
    
    print(f"Запись: Темп={temp} | Влажн={hum} | Давл={press}")
    
    time.sleep(5)