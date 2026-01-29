from airflow.providers.telegram.hooks.telegram import TelegramHook

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