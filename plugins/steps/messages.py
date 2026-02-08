from airflow.providers.telegram.hooks.telegram import TelegramHook
import numpy as np
import pandas as pd


def send_telegram_success_message(context):
    # Указываем токен и чат один раз при создании хука
    hook = TelegramHook(
        token='8200290320:AAEHcWGQhBfw9n7i4poC9RRmt4FHY6LGwt4', 
        chat_id='-1003817439977'
    )
    
    dag_name = context['dag'].dag_id # Получаем именно строку-идентификатор
    run_id = context['run_id']
    
    message = f'Исполнение DAG {dag_name} с id={run_id} прошло успешно!'
    
    # В send_message теперь достаточно передать только текст
    hook.send_message({'text': message})

def send_telegram_failure_message(context):
    hook = TelegramHook(
        token='8200290320:AAEHcWGQhBfw9n7i4poC9RRmt4FHY6LGwt4', 
        chat_id='-1003817439977'
    )
    
    dag_name = context['dag'].dag_id
    run_id = context['run_id']
    task_instance_key_str = context['task_instance_key_str']
    
    message = (
        f'Исполнение DAG {dag_name} с id={run_id} завершилось с ошибкой!\n'
        f'ID задачи: {task_instance_key_str}'
    )
    
    hook.send_message({'text': message})

def remove_duplicates(data):
    feature_cols = data.columns.drop('customer_id').tolist()
    is_duplicated_features = data.duplicated(subset=feature_cols, keep=False)
    data = data[~is_duplicated_features].reset_index(drop=True)
    return data

def fill_missing_values(data):

    cols_with_nans = data.isnull().sum()
    cols_with_nans = cols_with_nans[cols_with_nans > 0].index.drop('end_date')

    for col in cols_with_nans:

        if data[col].dtype in [float, int]:
            fill_value = data[col].mean()
        elif data[col].dtype == 'object':
            fill_value = data[col].mode().iloc[0]

        data[col] = data[col].fillna(fill_value)

    return data

def remove_outliers(data):
    num_cols = data.select_dtypes(['float']).columns
    threshold = 1.5
    potential_outliers = pd.DataFrame()

    for col in num_cols:
        Q1 = data[col].quantile(0.25)
        Q3 = data[col].quantile(0.75)
        IQR = Q3 - Q1
        margin = threshold*IQR
        lower = Q1 - margin
        upper = Q3 + margin
        potential_outliers[col] = ~data[col].between(lower, upper)
        outliers = potential_outliers.any(axis=1)
    data = data[~outliers].reset_index(drop=True)
    return data