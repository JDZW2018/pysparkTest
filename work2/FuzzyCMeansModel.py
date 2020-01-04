import numpy as np
import math
import random
import logging
from pyspark.sql import SparkSession

def get_logger(self):
    return self.logger


def set_logger(self, tostdout=False, logfilename=None, level=logging.WARNING):
    if tostdout:
        self.logger.addHandler(logging.StreamHandler())
    if logfilename:
        self.logger.addHandler(logging.FileHandler(logfilename))
    if level:
        self.logger.setLevel(level)

if __name__ == '__main__':
    spark = SparkSession.builder.appName("work2").getOrCreate()


