from json import loads
from pyspark.sql import SparkSession

config = loads(open('./config.json').read())['db']
url = "jdbc:{adapter}://{host}:{port}/{database}?user={username}&password={password}".format(**config)

spark = SparkSession\
    .builder\
    .appName('postgres test')\
    .config('spark.jars', './postgresql-42.2.6.jar')\
    .getOrCreate()

query = '(select count(*) as a from test.sales) as a'

sales = spark\
    .read\
    .format('jdbc')\
    .options(
        url=url,
        database=config['database'],
        dbtable=query,
        driver='org.postgresql.Driver')\
    .load()

output = sales

output\
    .write\
    .format('jdbc')\
    .options(
        url=url,
        database=config['database'],
        dbtable='test.pyspark_tests',
        driver='org.postgresql.Driver')\
    .mode('append')\
    .save()
