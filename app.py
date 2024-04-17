import string
import nltk
nltk.download('punkt')
from nltk import word_tokenize
from nltk.probability import FreqDist
from flask import Flask, render_template, request
from sklearn.feature_extraction.text import TfidfVectorizer
import pandas as pd
import logging
from math import log10
from collections import Counter
import psycopg2
from psycopg2 import sql, extensions

app = Flask(__name__)

def create_server_connection(host_name, user_name, user_password):
    connection = None
    try:
        connection = psycopg2.connect(
            host=host_name,
            user=user_name,
            password=user_password,
            port=5432
        )
        connection.set_session(autocommit=True)  
        print("PostgreSQL Database connection successful")
    except Exception as err:
        print(f"Error: '{err}'")

    return connection


def create_database(connection, query):
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        print("Database created successfully")
    except Exception as err:
        print(f"Error: '{err}'")

def create_table(connection, query):
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        connection.commit()
        print("Table created successfully")
    except Exception as err:
        print(f"Error: '{err}'")

def execute_query(connection, query):
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        connection.commit()
        print("Query successful")
    except Exception as err:
        print(f"Error: '{err}'")

def read_query(connection, query):
    cursor = connection.cursor()
    result = None
    try:
        cursor.execute(query)
        result = cursor.fetchall()
        return result
    except Exception as err:
        print(f"Error: '{err}'")

def execute_list_query(connection, sql, val):
    cursor = connection.cursor()
    try:
        cursor.executemany(sql, val)
        connection.commit()
        print("Query successful")
    except Exception as err:
        print(f"Error: '{err}'")

def calculate_tfidf(text):
    text = text.lower()
    spec_chars = string.punctuation + '\n\xa0«»\t-...'
    text = ''.join([ch for ch in text if ch not in spec_chars])
    text_tokens = word_tokenize(text)
    tokens = len(text_tokens)
    text = nltk.Text(text_tokens)
    fdist = FreqDist(text)
    top = fdist.most_common(50)
    pw = "admin"
    db = "words"

    connection = psycopg2.connect(
        host="127.0.0.1",
        user="postgres",
        password=pw,
        database=db,
        port=5432
    )
    insert_query = """
        INSERT INTO simple VALUES ('%s', %s, %s);
        """
    for i in range(len(top)):
        execute_query(connection, insert_query % (top[i][0], top[i][1]/tokens, log10(1/top[i][1])))
    return


@app.route('/')
def upload_file():
    return render_template('upl.html')


@app.route('/result', methods=['POST'])
def result():
    logging.basicConfig(level=logging.DEBUG)
    file = request.files['file']
    if file:
        text = file.read().decode("utf-8")
        pw = "admin"
        db = "words"
        connection = create_server_connection("127.0.0.1", "postgres", pw)
        create_database(connection, f"CREATE DATABASE {db}")
        connection.close()


        connection = psycopg2.connect(
            host="127.0.0.1",
            user="postgres",
            password=pw,
            database=db,
            port=5432
        )
        connection.set_session(autocommit=True)
        create_table(connection, """
        CREATE TABLE simple (
        word VARCHAR PRIMARY KEY,
        tf FLOAT,
        idf FLOAT
        );
        """)
        calculate_tfidf(text)
        results = read_query(connection, "SELECT * FROM simple")
        from_db = [list(result) for result in results]
        columns = ["word", "tf", "idf"]
        df = pd.DataFrame(from_db, columns=columns)
        return render_template('res.html', tables=[df.to_html(classes='data', header=True)])
    else:
        return "Файл не загружен"


if __name__ == '__main__':
    app.run(debug=True)