# tests/test_tags.py

def dml_test(test_func):
    """Декоратор для пометки теста как DML."""
    test_func.tags = ['dml']
    return test_func

def ddl_test(test_func):
    """Декоратор для пометки теста как DDL."""
    test_func.tags = ['ddl']
    return test_func
