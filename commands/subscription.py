# commands/subscription.py

from utils.config_loader import load_config
from utils.execute import execute_sql
import click

def setup_subscription_with_options(options):
    """Настройка подписки с заданными опциями."""
    try:
        config = load_config()
        master = config['master']
        replica = config['replica1']
        if not master or not replica:
            click.echo("Не удалось найти мастер или реплику в конфигурации.")
            return

        conn_params = replica['conn_params']
        master_conn_params = master['conn_params']

        # Формируем строку опций
        options_str = ', '.join([f"{key} = {value}" for key, value in options.items()])

        # Создание подписки с опциями
        create_subscription_query = f"""
            CREATE SUBSCRIPTION sub_{replica['name']}
            CONNECTION 'host={master_conn_params['host']} port={master_conn_params['port']} user={master_conn_params['user']} password={master_conn_params['password']} dbname={master_conn_params['dbname']}'
            PUBLICATION pub
            WITH ({options_str});
        """
        execute_sql(conn_params, create_subscription_query, server_name=replica['name'])
        click.echo(f"Подписка на {replica['name']} успешно создана с опциями: {options_str}")

    except Exception as e:
        click.echo(f"Ошибка при настройке подписки на {replica['name']}: {e}")
