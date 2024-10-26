# utils/subscription_decorator.py

import unittest
from functools import wraps

def subscription_options(options_list):
    """
    Декоратор для параметризации тестов с различными опциями подписки.
    :param options_list: список словарей с опциями подписки
    """
    def decorator(test_func):
        @wraps(test_func)
        def wrapper(self, *args, **kwargs):
            for options in options_list:
                with self.subTest(subscription_options=options):
                    self.setup_subscription_with_options(options)
                    test_func(self, *args, **kwargs)
                    self.clean_subscription()
        return wrapper
    return decorator
