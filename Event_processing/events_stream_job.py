from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp, window
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    TimestampType,
    LongType,
)

# 1) Create SparkSession
# نسوي سبارك سيشن ونضيف باكجات كافكا + هادوب
spark = (
    SparkSession.builder.appName("EngagementEventsStreaming")
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,"
        "org.apache.spark:spark-token-provider-kafka-0-10_2.12:3.4.1,"
        "org.apache.hadoop:hadoop-client-runtime:3.3.4,"
        "org.apache.hadoop:hadoop-client-api:3.3.4",
    )
    # نخلي عدد البارتيشنز قليل عشان نقدر نقرأ المخرجات بسهولة
    .config("spark.sql.shuffle.partitions", "1")
    .getOrCreate()
)

# نخفف كلام اللوق
spark.sparkContext.setLogLevel("WARN")

# 2) Kafka config
# إعدادات اتصال سبارك بكافكا
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "engagement_stream"

# 3) Read from Kafka as streaming
# نقرأ ستريم مباشر من توبك كافكا
raw_df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "latest")  # نبدأ من أحدث الأحداث فقط
    .load()
)

# 4) Define JSON schema
# هذا شكل البيانات اللي جوّا value في كافكا
event_schema = StructType(
    [
        StructField("event_id", StringType(), True),
        StructField("content_id", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("event_type", StringType(), True),
        # نحفظها كنص أول، بعدين نحولها Timestamp
        StructField("event_ts", StringType(), True),
        StructField("duration_ms", LongType(), True),
        StructField("device", StringType(), True),
        # raw_payload هنا بس كنص بسيط (مو JSON nested)
        StructField("raw_payload", StringType(), True),
    ]
)

# 5) Parse JSON from Kafka value
# نفك JSON اللي في value ونحوله لأعمدة عادية
events_df = (
    raw_df.selectExpr("CAST(value AS STRING) as value_str")
    .select(from_json(col("value_str"), event_schema).alias("data"))
    .select("data.*")
    # نحول event_ts من نص إلى تايمستامب حقيقي
    .withColumn("event_ts", to_timestamp("event_ts"))
)

# 6) Simple aggregation example
# نحسب عدد الأحداث لكل (محتوى + نوع حدث) داخل نافذة زمنية دقيقة
agg_df = events_df.groupBy(
    window(col("event_ts"), "1 minute"),  # نافذة متحركة كل دقيقة
    col("content_id"),
    col("event_type"),
).count()

# 7) Write to console sink
# نطبع النتائج في الكونسول كستريم للتجربة والمتابعة
query = (
    agg_df.writeStream.outputMode("update")  # نحدّث الصفوف بدل ما نعيدها من جديد
    .format("console")
    .option("truncate", "false")  # ما نقصّ الأعمدة الطويلة
    .start()
)

# 8) Keep stream running
# نخلي الجوب شغالة لين نوقفها يدوي
query.awaitTermination()
