import os
import requests
import pandas as pd
import mysql.connector
from dotenv import load_dotenv

load_dotenv()

# MySQL connection details
MYSQL_HOST = os.getenv("MYSQL_HOST")
MYSQL_PORT = os.getenv("MYSQL_PORT")
MYSQL_DATABASE = os.getenv("MYSQL_DATABASE")
MYSQL_USER = os.getenv("MYSQL_USER")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD")

baseurl = 'https://rickandmortyapi.com/api/'

endpoint = "character"

def main_request(baseurl, endpoint, page_no):
    r = requests.get(baseurl + endpoint + f'?page={page_no}')
    return r.json()

def get_pages(response):
    return response['info']['pages']

def parse_json(response):
    charlist = []

    for item in response['results']:
        char = {
            'id': item['id'],
            'name': item['name'],
            'no_episodes': len(item['episode'])
        }
        charlist.append(char)
    return charlist

mainlist = []
data = main_request(baseurl, endpoint, 1)

for x in range(1, get_pages(data) + 1):
    mainlist.extend(parse_json(main_request(baseurl, endpoint, x)))

print(mainlist[:5])

# Connect to MySQL
conn = mysql.connector.connect(
    host=MYSQL_HOST,
    port=MYSQL_PORT,
    user=MYSQL_USER,
    password=MYSQL_PASSWORD,
    database=MYSQL_DATABASE
)

cursor = conn.cursor()

# Create table if it does not exist
create_table_query = """
CREATE TABLE IF NOT EXISTS characters (
    id INT PRIMARY KEY,
    name VARCHAR(255),
    no_episodes INT
);
"""
cursor.execute(create_table_query)

# Insert data into table
insert_query = """
INSERT INTO characters (id, name, no_episodes)
VALUES (%s, %s, %s)
ON DUPLICATE KEY UPDATE
    name = VALUES(name),
    no_episodes = VALUES(no_episodes);
"""

for char in mainlist:
    cursor.execute(insert_query, (char['id'], char['name'], char['no_episodes']))

# Commit changes and close connection
conn.commit()
cursor.close()
conn.close()

print("Data inserted into MySQL successfully!")
