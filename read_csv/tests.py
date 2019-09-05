from pyspark.sql import SparkSession, SQLContext


ss = SparkSession.builder.appName("myApp").getOrCreate()
csv_file = ss.read.csv('./data.csv', header=True)

print(csv_file.show())
print(csv_file.where('name like \'%e%\'').show())
