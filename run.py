from __future__ import print_function
import sys
from pyspark import SparkContext

# init contex
sc = SparkContext(appName="NatesSparkathon")
# load dataset
text_file = sc.textFile("C:\Users\Nathaniel\Documents\Projects\Sparkathon\datasets\ghcnd_all.zip")
#text_file = sc.textFile("C:\Users\Nathaniel\Documents\Projects\Sparkathon\datasets\ghcnd_all\ACW00011604.dly")
# select SNOW
snow = text_file.filter(lambda line: "SNOW" in line)
# execute
count = snow.count()
print(count)

# terminate
sc.stop()