from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType
from pyspark.sql.functions import col, expr, lit, when, regexp_replace, to_date, coalesce

#from pyspark.sql.types import _parse_datatype_string
"""st = "name string,age int"
str = _parse_datatype_string(st
"""


#ayush = spark.getActiveSession
# Initialize Spark session
spark = (
    SparkSession
    .builder
    .appName("New")
    .master("local[*]")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("Error")

# Correct Employee data with integers for age and salary
emp_data = [
    ["1", "101", "John", 30, "Male", 47000, "2015-01-03"],
    ["2", "107", "Harry", 26, "Male", 95000, "2018-06-23"],
    ["3", "107", "Ollie", 38, "Male", 36000, "2009-07-23"],
    ["4", "103", "Johnny", 25, "Male", 81000, "2015-08-23"],
    ["5", "102", "Robbie", 23, "Female", 77000, "2014-07-23"],
    ["6", "101", "Sam", 29, "Male", 87000, "2018-04-03"],
    ["7", "103", "Henry", 27, "Male", 29000, "2019-06-23"]
]

# Define schema with appropriate data types
emp_schema = StructType([
    StructField("id", StringType(), True),
    StructField("department", StringType(), True), #nullable = true 4
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("gender", StringType(), True),
    StructField("salary", IntegerType(), True),
    StructField("date", StringType(), True)  # Temporarily keeping date as string
])

# Create DataFrame
emp_df = spark.createDataFrame(data=emp_data, schema=emp_schema)

emp_df.rdd.getNumPartitions()

# Convert date column to DateType
emp_df = emp_df.withColumn("date", col("date").cast(DateType()))

#emp = emp_df.select(expr("id as emp_id"),expr("cast(age as int) as age"),col("age") .cast(IntegerType()))
#emp = emp_df.selectExpr("id as emp_id","cast(age as int) as age","salary")

#emp_df.write.csv("output/emp_data.csv", header=True, mode='overwrite')

# Show DataFrame
emp_df.show()

#emp_df.write.format("csv").save("data/output/emp.csv")
# Print schema

emp_df.printSchema()
print(emp_df)

higher_salary = emp_df.where("salary > 50000").select(col("id"),expr("department"),emp_df.age,col("salary"),"name")
higher_salary.show()

df = spark.read.csv(r"C:\Users\Asus\PycharmProjects\pythonProject3\data\simple-zipcodes.csv",header=True,inferSchema=True)
df.createOrReplaceTempView("zipcodes")

emp_df.createOrReplaceTempView("empdf")

joineddf = spark.sql("select empdf.*,zipcodes.country from empdf join zipcodes on empdf.id = zipcodes.id")
joineddf.show()

newcol = joineddf.withColumn("tax",col("salary")* 0.2).withColumn("GlobalID",lit("hdhdfhhj47"))\
            .withColumn("AppCode",lit(1407)).withColumnRenamed("id","emp_id")
newcol.show(6)  #.limt(5)
# suppose we need to drop a column or more example empdf.drop("salary","age")
# suppose we need to add a common value to a table example global id = jneerjje32nn , we use lit

# suppose we want to add multiple columns in one time we can create a dict
columns = {
    "tax" : col("salary") * 0.3,
    "globalId" : lit("uic48")
}
employee = emp_df.withColumns(columns)
employee.show()
employee = employee.withColumn("Gen", when(col("gender") == "Male","M")
                               .when(col("gender") == "Female",'F').otherwise(None)).withColumn("new_name", regexp_replace(col("name"),'J','Z'))
"""employee = employee.withColumn("Gen",expr(case when gender = 'Male' then 'M' when gender ='Female' then
            'F' else null"""
#emp.withColumn("date",to_date(col("date"), yyyy-MM-dd)).withColumn("new_date",current_date())
employee.show()

#drop null value
emp_1 = employee.na.drop()
"""coalesce is used to either:

Reduce the number of partitions: This version of coalesce is used to reduce the number of partitions in a DataFrame
 or RDD, which can be helpful for optimizing performance during certain operations, 
 like saving data to disk. 

# Reduce the number of partitions from 10 to 4
df_coalesced = df.coalesce(4)
Substitute null values as below
"""
emp_2 = employee.withColumn("Gen", coalesce("Gen",lit("homo")))

