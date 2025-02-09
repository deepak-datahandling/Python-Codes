# Create DataFrame
import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName('your app name') \
    .getOrCreate()

data = [
("James", "zen", "Smith", "1991-04-01", "M", 3000),
("Michael", "Rose", "zwee", "2000-05-19", "M", 4000),
("Robert", "", "Williams", "1978-09-05", "M", 4000),
("Maria", "Anne", "Jones", "1967-12-01", "F", 4000),
("Jen", "Mary", "Brown", "1980-02-17", "F", 400),
]
columns = ["firstname", "middlename", "lastname", "dob", "gender", "salary"]
df = spark.createDataFrame(data=data, schema=columns)
df.show()