#utils/tesstHandler.py

import unittest
from utils.log_handler import logger

class TestHandler(unittest.TextTestResult):
    def __init__(self, *args, **kwargs):
        super(TestHandler, self).__init__(*args, **kwargs)
        self.successes = []
        self.failures_list = []
        self.errors_list = []
        self.skipped_list = []

    def startTest(self, test):
        super().startTest(test)
        print(f"Running {test.id()}... ", end='')

    def addSuccess(self, test):
        super().addSuccess(test)
        self.successes.append(test)
        print("OK")

    def addFailure(self, test, err):
        super().addFailure(test, err)
        self.failures_list.append(test)
        print("FAIL")

        error_message = self._exc_info_to_string(err, test)
        logger.error(f"FAIL: {test.id()}\n{error_message}")

    def addError(self, test, err):
        super().addError(test, err)
        self.errors_list.append(test)
        print("ERROR")
        error_message = self._exc_info_to_string(err, test)
        logger.error(f"ERROR: {test.id()}\n{error_message}")

    def addSkip(self, test, reason):
        super().addSkip(test, reason)
        self.skipped_list.append(test)
        print("SKIPPED")
        logger.info(f"SKIPPED: {test.id()} - {reason}")
