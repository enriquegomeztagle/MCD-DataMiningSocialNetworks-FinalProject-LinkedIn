# %%
# %pip install pyspark==3.5.1 neo4j

# %%
url = "neo4j://localhost:7687"
username = "neo4j"
password = "password"
dbname = "neo4j"

# %%
from pyspark.sql import SparkSession


def create_spark_session():
    return (
        SparkSession.builder.appName("Neo4jIntegration")
        .config(
            "spark.jars",
            "../utils/jars/neo4j-connector-apache-spark_2.12-5.3.8_for_spark_3.jar",
        )
        .config("spark.neo4j.url", "bolt://localhost:7687")
        .config("spark.neo4j.authentication.type", "basic")
        .config("spark.neo4j.authentication.basic.username", username)
        .config("spark.neo4j.authentication.basic.password", password)
        .config("neo4j.url", url)
        .config("neo4j.authentication.type", "basic")
        .config("neo4j.authentication.basic.username", username)
        .config("neo4j.authentication.basic.password", password)
        .config("neo4j.database", dbname)
        .getOrCreate()
    )


try:
    spark
except NameError:
    print("🔄 No existing SparkSession found. Creating a new one...")
    spark = create_spark_session()
    print("✅ SparkSession created successfully.")
else:
    if spark._jsparkSession is None:
        print("⚠️ Existing SparkSession is not active. Recreating it...")
        spark = create_spark_session()
        print("✅ SparkSession re-created successfully.")
    else:
        print("✅ SparkSession is already active.")

print(f"🔥 Spark is running — version: {spark.version}")

# %%
print("List of jars:")
print(spark.sparkContext._jsc.sc().listJars())

# %%
df = spark.read.format("org.neo4j.spark.DataSource").option("labels", "User").load()

df.show()

# %%
