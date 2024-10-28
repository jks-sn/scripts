# utils/subscription_decorator.py

import unittest
from functools import wraps

from utils.repliaction_utils import create_subscription, drop_subscription

def subscription_options(options_list):
    """
    Декоратор для параметризации тестов с различными опциями подписки.
    """
    def decorator(test_func):
        @wraps(test_func)
        def wrapper(self, *args, **kwargs):
            for options in options_list:
                with self.subTest(subscription_options=options):
                    # Создаем подписку с текущими опциями
                    connection_info = f'host=localhost port={self.master["port"]} dbname=postgres user=postgres'
                    subscription_name = f"sub_{self.replica['name']}_{test_func.__name__}"

                    # Удаляем подписку, если она уже существует
                    drop_subscription(self.replica['conn_params'], subscription_name, self.replica['name'])

                    create_subscription(
                        conn_params=self.replica['conn_params'],
                        subscription_name=subscription_name,
                        connection_info=connection_info,
                        publication_name=f'pub_{self.master["name"]}',
                        server_name=self.replica['name'],
                        options=options
                    )
                    # Подождем некоторое время для инициации подписки
                    self.wait_for_subscription(subscription_name)
                    # Устанавливаем текущие опции для использования в тесте
                    self.current_subscription_options = options
                    # Выполнение теста
                    test_func(self, *args, **kwargs)
                    # Очистка подписки
                    drop_subscription(self.replica['conn_params'], subscription_name, self.replica['name'])
        return wrapper
    return decorator
