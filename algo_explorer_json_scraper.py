import json 
import requests
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import pendulum

print("start run")

start_date = pendulum.datetime(2023, 1, 8, 6)
schedule_interval = timedelta(days=7)

default_args={
        "depends_on_past": False,
        "email": ["airflow@example.com"],
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 4,
        "retry_delay": timedelta(hours=1),
}

def json_scraper(url, file_name):
    
    response = requests.request("GET", url)
    json_data = response.json()
    
    with open(file_name, "w", encoding="utf-8") as json_file:
        json.dump(json_data, json_file, ensure_ascii=False, indent=4)
    
        

def json_scraper_loop():
    pages_remaining = True
    page = 1
    current_json = "page_1.json"
    
    json_scraper("https://algoindexer.algoexplorerapi.io/v2/assets/27165954/balances", "page_1.json")
    
    
    while pages_remaining == True:
        try:
            file = open(current_json)
            data = json.load(file)
            
            data
            base_url = "https://algoindexer.algoexplorerapi.io/v2/assets/27165954/balances"
    
            page += 1
            new_url = base_url[:] + "?" + "next=" + data["next-token"]
            file.close()
            current_json = "page_" + str(page) + ".json"
    
            json_scraper(new_url, current_json)
        except:
            break
                
        
with DAG(
    "raw_address_jsons",
    default_args=default_args,
    description="",
    schedule_interval=schedule_interval,
    start_date=start_date, 
    catchup=False,
) as dag:
    
    extract_adresses = PythonOperator(
        task_id = "extract_adresses",
        python_callable=json_scraper_loop,
        dag=dag
    )

    ready = DummyOperator(task_id="ready")
    
    extract_adresses >> ready


        
print("end run")