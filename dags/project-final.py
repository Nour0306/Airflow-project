from datetime import timedelta

import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import requests
import pymongo
from datetime import datetime


def generate_json():
    url_profile = "https://financialmodelingprep.com/api/v3/profile/AAPL?apikey=fe72549f17263ca8f71ab7e3b997c502"
    url_rating = "https://financialmodelingprep.com/api/v3/rating/AAPL?apikey=fe72549f17263ca8f71ab7e3b997c502"

    r_profile = requests.get(url_profile)
    r_rating = requests.get(url_rating)

    profile_json = r_profile.json()[0]
    rating_json = r_rating.json()[0]

    json_merge = {**profile_json, **rating_json}
    json_merge['timestamp'] = datetime.now().isoformat()
    return json_merge


def insert_json(json):
    myclient = pymongo.MongoClient("mongodb://mongodb:27017/")
    db = myclient.financial
    collection = db.apple
    collection.insert_one(json)


def do_all():
    json = generate_json()
    insert_json(json)


default_args = {
    'id': 'projet-version-1',
    'owner': 'airflow',
}

dag = DAG(
    'projet-version-1',
    default_args=default_args,
    description='Projet APPLE v1',
    schedule_interval='*/1 * * * *',
    start_date=airflow.utils.dates.days_ago(0),
    catchup=False
)

task = PythonOperator(task_id="Ajout-mongo", python_callable=do_all, dag=dag)

task
