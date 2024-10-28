# tests/test_tags.py

def ddl_test(test_func):
    """Декоратор для DDL"""
    test_func.tags = ['ddl']
    return test_func

def cascade_dml_test(test_func):
    """Декоратор для пометки теста как каскадный логический DML."""
    test_func.tags = ['cascade_dml']
    return test_func

def cascade_ddl_test(test_func):
    """Декоратор для пометки теста как какскадный логический DDL."""
    test_func.tags = ['cascade_ddl']
    return test_func

def logical_ddl_test(test_func):
    """Декоратор для пометки теста как логический DDL."""
    test_func.tags = ['logical_ddl']
    return test_func
