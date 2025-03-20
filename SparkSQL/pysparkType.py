from pyspark.sql import SparkSession, Row
from pyspark.sql.types import *
from datetime import datetime

"""
PYSPARK TYPE
        ===BASIC===
- StringType: string
- IntegerType: int 32bit
- LongType: int 64bit
- FloatType: float 32bit
- DoubleType: float 64bit
- TimestampType: datetime
- DateType: date
- ByteType: int 8bit
- ShortType: int 16bit
- BinaryType: bytearray
- BooleanType: bool

        ===ADVANCED===
- StructType: list of StructField
    - StructField: name, dataType, nullable

- ArrayType(elementType): list of ArrayType
    - elementType: DataType
    
- MapType(keyType, valueType): list of MapType
    - keyType: DataType
    - valueType: DataType
"""

spark = SparkSession.builder \
    .appName("SparkSQL") \
    .master("local[*]") \
    .config("spark_executor_memory", "4g") \
    .getOrCreate()

# sc = spark.sparkContext

data = ([
    Row(
        id = 1,
        name = "Nguyen Khang",
        age = 20,
        salary = 3000.0,
        is_active = True,
        scores = [1,2,3,4,5],
        attributes = {"dept": "Massage", "role": "BJ"},
        hire_date = datetime.strptime("2020-01-01", "%Y-%m-%d"),
        last_login = datetime.strptime("2020-01-01 22:30:15", "%Y-%m-%d %H:%M:%S"),
        tax_rate = 45.67
    ),
Row(
        id = 2,
        name = "Duc Thinh",
        age = 20,
        salary = 3000.0,
        is_active = True,
        scores = [1,2,3,4,5],
        attributes = {"dept": "Massage", "role": "BJ"},
        hire_date = datetime.strptime("2020-01-01", "%Y-%m-%d"),
        last_login = datetime.strptime("2020-01-01 22:30:15", "%Y-%m-%d %H:%M:%S"),
        tax_rate = 45.67
    )
])

schemaEmployees = StructType(
    [
        StructField("id", IntegerType(), False),
        StructField("name", StringType(), True),
        StructField("age", IntegerType(), True),
        StructField("salary", FloatType(), True),
        StructField("is_active", BooleanType(), True),
        StructField("scores", ArrayType(IntegerType()), True),
        StructField("attributes", MapType(StringType(), StringType()), True),
        StructField("hire_date", DateType(), True),
        StructField("last_login", TimestampType(), True),
        StructField("tax_rate", FloatType(), True)
    ]
)

df = spark.createDataFrame(data, schemaEmployees)

df.show()
df.printSchema()