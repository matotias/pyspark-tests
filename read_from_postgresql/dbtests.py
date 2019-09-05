from json import loads
from pyspark.sql import SparkSession

config = loads(open('./config.json').read())
url = "jdbc:{adapter}://{host}:{port}/{database}?user={username}&password={password}".format(**config)

spark = SparkSession\
    .builder\
    .appName('postgres test')\
    .config('spark.jars', './postgresql-42.2.6.jar')\
    .getOrCreate()

df = spark\
    .read\
    .format('jdbc')\
    .options(
        url=url,
        database=config['database'],
        dbtable='test.sales',\
        driver='org.postgresql.Driver')\
    .load()

print(df.printSchema())
