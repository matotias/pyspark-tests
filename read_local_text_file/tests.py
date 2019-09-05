from pyspark import SparkContext


sc = SparkContext()
text_file = sc.textFile('exampleFile.txt')
print(text_file.count())
print(text_file.first())

SEC_FIND = text_file.filter(lambda line: 'second' in line)
print(SEC_FIND.collect())  # collect -> turn RDD to in memory list
print(SEC_FIND.count())

print(text_file.collect())
print(text_file.map(lambda x: x.split()).collect())
print(text_file.flatMap(lambda x: x.split()).collect())

print(text_file.union(text_file.flatMap(lambda x: x.split())).collect())
