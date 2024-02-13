import telegram
import matplotlib.pyplot as plt
import seaborn as sns
import io
import pandas as pd
import pandahouse as ph
from datetime import datetime, timedelta, date

from airflow.decorators import dag, task

connection = {'host': 'https://clickhouse.lab.karpov.courses',
    'password': 'dpo_python_2020',
    'user': 'student',
    'database': 'simulator'
                     }

default_args = {
    'owner': 'a-salmanova',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 10, 12),
}

schedule_interval = '0 11 * * *'

def get_df(query, connection):
    df = ph.read_clickhouse(query=query, connection=connection)
    
    return df

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def asalmanova_dag_report():
    @task()
    def extract_feed(connection):
        # Получаем данные по показателям ленты за прошедшие 7 дней
        query = '''
        SELECT
            toDate(time) as report_date,
            os,
            source,
            COUNT(DISTINCT user_id) as DAU,
            COUNT(DISTINCT post_id) as posts,
            COUNT(user_id)/COUNT(DISTINCT user_id) as apu,
            countIf(action='like')/countIf(action='view') as ctr
        FROM simulator_20231220.feed_actions
        WHERE toDate(time) between yesterday()-7 and yesterday()
        GROUP BY report_date, os, source
        '''
        feed_df = get_df(query, connection)
        
        return feed_df
    
    @task()
    def extract_messages(connection):
        # Получаем данные по показателям сообщений за прошедшие 7 дней
        query = '''
        SELECT toDate(time) as report_date,
                os,
                source,
                COUNT(user_id) as messages,
                COUNT(DISTINCT user_id) as users_message,
                COUNT(user_id)/COUNT(DISTINCT user_id) as mpu
        FROM simulator_20231220.message_actions
        WHERE toDate(time) between yesterday()-7 and yesterday()
        GROUP BY report_date, os, source
        '''
        message_df = get_df(query, connection)
        
        return message_df
    
    @task()
    def extract_joined_metrics(connection):
        # Получаем данные по показателям активных пользователей (и с лентой, и с сообщениями) за прошедшие 7 дней
        query = '''
            SELECT toDate(time) as report_date, COUNT(DISTINCT user_id) as active_users
            FROM simulator_20230920.feed_actions a
                JOIN simulator_20230920.message_actions b
                ON toDate(a.time)=toDate(b.time)
                AND a.user_id = b.user_id
            WHERE toDate(a.time) between yesterday()-7 and yesterday()
            AND toDate(b.time) between yesterday()-7 and yesterday()
            GROUP BY report_date
            '''

        active_df = get_df(query, connection)

        return active_df

    @task()
    def merge_feed_message(feed_df, message_df):
        # Объединим датафреймы ленты и сообщений
        df = feed_df.merge(message_df, on=['report_date', 'os', 'source'])

        return df

    @task()
    def get_agg_df(df, active_df):
        # Создадим датафрейм без разбивки на OS, source
        df_all_1 = df.groupby(['report_date']).sum().reset_index()[['report_date','DAU','posts','messages','users_message']]
        df_all_2 = df.groupby(['report_date'])[['apu','ctr','mpu']].mean().reset_index()
        df_all = df_all_1.merge(df_all_2, on=['report_date'])
        # Добавим датафрейм с активными пользователями
        df_all = df_all.merge(active_df, on=['report_date'])

        return df_all

    @task()
    def generate_message(df_all):
        # Создадим датафрейм только за последний актуальный день
        df_yesterday = df_all[df_all['report_date'] == df_all['report_date'].max()]

        # Получаем последнюю актуальную дату и дату перед ней
        report_date = df_yesterday['report_date'].dt.date.values[0].strftime('%d.%m.%y')
        previous_date = df_all['report_date'].max() - timedelta(days=1)

        # Сохраняем основные метрики в переменные
        users_action = int(df_yesterday['DAU'].values[0])
        posts = int(df_yesterday['posts'].values[0])
        apu = int(df_yesterday['apu'].values[0])
        ctr = float(df_yesterday['ctr'].values[0].round(2))
        messages = int(df_yesterday['messages'].values[0])
        users_message = int(df_yesterday['users_message'].values[0])
        mpu = int(df_yesterday['mpu'].values[0])
        active_users = int(df_yesterday['active_users'].values[0])

        # Сохраняем эти же метрики за предыдущий день (позавчера)
        users_action_prev = int(df_all[df_all['report_date'] == previous_date]['DAU'].values[0])
        posts_prev = int(df_all[df_all['report_date'] == previous_date]['posts'].values[0])
        apu_prev = int(df_all[df_all['report_date'] == previous_date]['apu'].values[0])
        ctr_prev = float(df_all[df_all['report_date'] == previous_date]['ctr'].values[0])
        messages_prev = int(df_all[df_all['report_date'] == previous_date]['messages'].values[0])
        users_message_prev = int(df_all[df_all['report_date'] == previous_date]['users_message'].values[0])
        mpu_prev = int(df_all[df_all['report_date'] == previous_date]['mpu'].values[0])
        active_users_prev = int(df_all[df_all['report_date'] == previous_date]['active_users'].values[0])

        # Считаем, как изменились метрики по отношению к предыдущему дню
        users_action_change = round(100*(users_action - users_action_prev)/users_action_prev,2)
        posts_change = round(100*(posts - posts_prev)/posts_prev,2)
        apu_change = round(100*(apu - apu_prev)/apu_prev,2)
        ctr_change = round(100*(ctr - ctr_prev)/ctr_prev,2)
        messages_change = round(100*(messages - messages_prev)/messages_prev,2)
        users_message_change = round(100*(users_message - users_message_prev)/users_message_prev,2)
        mpu_change = round(100*(mpu - mpu_prev)/mpu_prev,2)
        active_users_change = round(100*(active_users - active_users_prev)/active_users_prev,2)

        # Формируем сообщение
        message = '''
        *Ежедневный отчет по приложению за {}*

        Всего активных\* пользователей: {} ({}%)

        _Лента_
        - _Пользователи с действиями:_ {} ({}%),
        - _Просмотренные посты:_ {} ({}%),
        - _Ср.кол-во действий на пользователя:_ {} ({}%),
        - _CTR:_ {} ({}%)

        _Сообщения_
        - _Всего сообщений:_ {} ({}%),
        - _Пользователи с отправленными сообщениями:_ {} ({}%),
        - _Ср. кол-во отправленных сообщений на пользователя:_ {} ({}%)

        \*Активный пользователь - пользователь и с отправленными сообщениями, и с действием'''.format(report_date,
                                           active_users, active_users_change,
                                           users_action, users_action_change,
                                           posts, posts_change,
                                           apu, apu_change,
                                           ctr, ctr_change,
                                           messages, messages_change,
                                           users_message, users_message_change,
                                           mpu, mpu_change)
        return message

    @task()
    def generate_graph(df, df_all):
        # Переводим дату в более читаемый формат
        df['report_date_v2'] = df['report_date'].dt.strftime('%d.%m')
        df_all['report_date_v2'] = df_all['report_date'].dt.strftime('%d.%m')

        # Получаем первую и последнюю дату отчета для названия файла
        report_date = df['report_date'].dt.date.values[0].strftime('%d.%m.%y')
        start_date = df[df['report_date'] == df['report_date'].min()]\
        ['report_date'].dt.date.values[0].strftime('%d.%m.%y')

        name = start_date + '-' + report_date

        sns.set_style('whitegrid')
        sns.set_palette('mako')
        fig, axes = plt.subplots(4, 2, figsize=(15,25))

        axes[0,0].set_title('Действия на пользователя', size=16)
        axes[0,1].set_title('Пользователи с действиями', size=16)
        axes[1,0].set_title('Пользователи с действиями по ОС', size=16)
        axes[1,1].set_title('Пользователи с действиями по источнику', size=16)
        axes[2,0].set_title('CTR', size=16)
        axes[2,1].set_title('Активные пользователи', size=16)
        axes[3,0].set_title('Сообщения', size=16)
        axes[3,1].set_title('Сообщения на пользователя', size=16)

        sns.lineplot(ax=axes[0,0],
                     data=df_all,
                     x='report_date_v2',
                     y='apu',
                     color='#4f354a').set(xlabel='', ylabel='')

        sns.lineplot(ax=axes[0,1],
                     data=df_all,
                     x='report_date_v2',
                     y='DAU',
                     color='#9a8c98').set(xlabel='', ylabel='')

        sns.barplot(ax=axes[1,0],
                    data=df.sort_values('report_date'),
                    x='report_date_v2',
                    y='DAU',
                    hue='os').set(xlabel='', ylabel='')

        sns.barplot(ax=axes[1,1],
                    data=df.sort_values('report_date'),
                    x='report_date_v2',
                    y='DAU',
                    hue='source').set(xlabel='', ylabel='')

        sns.lineplot(ax=axes[2,0],
                     data=df_all,
                     x='report_date_v2',
                     y='ctr',
                     color='#3891a6').set(xlabel='', ylabel='')

        sns.lineplot(ax=axes[2,1],
                     data=df_all,
                     x='report_date_v2',
                     y='active_users',
                     color='#0d5c63').set(xlabel='', ylabel='')

        sns.lineplot(ax=axes[3,0],
                     data=df_all,
                     x='report_date_v2',
                     y='messages',
                     color='#3891a6').set(xlabel='', ylabel='')

        sns.lineplot(ax=axes[3,1],
                     data=df_all,
                     x='report_date_v2',
                     y='mpu',
                     color='#4f354a').set(xlabel='', ylabel='')

        plot_object = io.BytesIO()
        plt.savefig(plot_object)
        plot_object.seek(0)
        plot_object.name = name + '.png'
        plt.close()

        return plot_object
    
    @task()
    def send_message(message, plot_object):
        # Отправляем короткое сообщение и графики
        my_token = '6537240588:AAEP4boufvw99c3_eRc7HEKa3W92e8lyi34' 
        bot = telegram.Bot(token=my_token) 
        chat_id = -938659451
        
        bot.sendMessage(chat_id=chat_id, text=message, parse_mode='Markdown')
        bot.sendPhoto(chat_id=chat_id, photo=plot_object)
    
    feed_df = extract_feed(connection)
    message_df = extract_messages(connection)
    active_df = extract_joined_metrics(connection)
    
    df = merge_feed_message(feed_df, message_df)
    df_all = get_agg_df(df, active_df)

    message = generate_message(df_all)
    plot_object = generate_graph(df, df_all)
    
    send_message(message, plot_object)

asalmanova_dag_report = asalmanova_dag_report()
