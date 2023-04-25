import pandahouse
from datetime import datetime, timedelta
import telegram
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import io
import pandas as pd

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

#параметры, которые прокидываются в таски 
default_args = {
    'owner': 'arogn',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 3, 22),
}

#cron-выражение, говорящее о том, что надо запускать DAG в 11:00 
schedule_interval = '0 11 * * *'

def arogn_report(): 
    
    connection = {
    'host': '',
    'password': '',
    'user': '',
    'database': ''
    }
    
    #запрос для получения данных за вчерашний день 
    q1 = ''' SELECT 
                count (distinct user_id) as dau
              , sum(action='like') as likes
              , sum(action='view') as views
              , likes/views as ctr 
             FROM table.f_a 
             WHERE toDate(time) = yesterday()'''

    df1 = pandahouse.read_clickhouse(q1, connection=connection)

    my_token = ''

    bot = telegram.Bot(token=my_token)

    chat_id = ''

    bot.sendMessage(chat_id=chat_id, text="DAU за вчера: " + str(df1.iloc[0,0]))
    bot.sendMessage(chat_id=chat_id, text="Просмотры за вчера: " + str(df1.iloc[0,2]))
    bot.sendMessage(chat_id=chat_id, text="Лайки за вчера: " + str(df1.iloc[0,1]))
    bot.sendMessage(chat_id=chat_id, text="CTR за вчера: " + str(df1.iloc[0,3]))
    
    #запрос для получения данных за последнюю неделю 
    q2 = ''' SELECT 
                toDate(time) as date
              , count (distinct user_id) as dau
              , sum(action='like') as likes
              , sum(action='view') as views
              , likes/views as ctr 
             FROM table.f_a
             WHERE (toDate(time) >= today() - 7) and (toDate(time) < today())
             GROUP BY toDate(time)'''

    df2 = pandahouse.read_clickhouse(q2, connection=connection)

    #график dau
    plt.figure(figsize=(10, 5))
    sns.lineplot(df2["date"], df2["dau"])
    plt.title('DAU за последнюю неделю')
    plot_object = io.BytesIO()
    plt.savefig(plot_object)
    plot_object.seek(0)
    plot_object.name = 'dau.png'
    plt.close()
    bot.sendPhoto(chat_id=chat_id, photo=plot_object)

    #график likes
    plt.figure(figsize=(10, 5))
    sns.lineplot(df2["date"], df2["likes"])
    plt.title('Количество лайков за последнюю неделю')
    plot_object = io.BytesIO()
    plt.savefig(plot_object)
    plot_object.seek(0)
    plot_object.name = 'likes.png'
    plt.close()
    bot.sendPhoto(chat_id=chat_id, photo=plot_object)

    #график views
    plt.figure(figsize=(10, 5))
    sns.lineplot(df2["date"], df2["views"])
    plt.title('Количество просмотров за последнюю неделю')
    plot_object = io.BytesIO()
    plt.savefig(plot_object)
    plot_object.seek(0)
    plot_object.name = 'views.png'
    plt.close()
    bot.sendPhoto(chat_id=chat_id, photo=plot_object)

    #график ctr
    plt.figure(figsize=(10, 5))
    sns.lineplot(df2["date"], df2["ctr"])
    plt.title('CTR за последнюю неделю')
    plot_object = io.BytesIO()
    plt.savefig(plot_object)
    plot_object.seek(0)
    plot_object.name = 'ctr.png'
    plt.close()
    bot.sendPhoto(chat_id=chat_id, photo=plot_object)
    

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def dag_arogn_report():
    
    @task()
    def make_arogn_report():
        arogn_report()
    
    make_arogn_report()

dag_arogn_report = dag_arogn_report()