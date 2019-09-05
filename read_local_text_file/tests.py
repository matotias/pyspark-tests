from pyspark import SparkContext


sc = SparkContext()
text_file = sc.textFile('exampleFile.txt')
print(text_file.count())
print(text_file.first())

SECFIND = text_file.filter(lambda line: 'second' in line)
print(SECFIND.collect())
print(SECFIND.count())
