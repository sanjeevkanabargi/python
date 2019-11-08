from __future__ import print_function

import sys
from operator import add

from pyspark.sql import SparkSession


if __name__ == "__main__":
    
    spark = SparkSession\
        .builder\
        .appName("PythonWordCount")\
        .getOrCreate()

    inputfile = "/Users/skanabargi/dataSource/firewall/cfw-pr.parquet"
    outputfile = "/Users/skanabargi/dataSource/firewall/cfw100.parquet"

    lines = spark.read.parquet(inputfile)
    

    lines.Take(5)write.parquet(outputfile)

    newdata.write.parquet(outputfile)
    
    spark.stop()