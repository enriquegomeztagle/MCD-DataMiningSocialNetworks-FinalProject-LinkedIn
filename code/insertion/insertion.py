# %%
url = "neo4j://localhost:7687"
username = "neo4j"
password = "password"
dbname = "neo4j"
jar_path = "../../utils/jars/neo4j-connector-apache-spark_2.12-5.3.8_for_spark_3.jar"
me = ""

# %%
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import (
    concat_ws,
    to_date,
    initcap,
    trim,
    regexp_replace,
    monotonically_increasing_id,
    lit,
    col,
    udf,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    FloatType,
    BooleanType,
    DateType,
    TimestampType,
    DateType,
    TimestampType,
)
from neo4j import GraphDatabase
import unicodedata, re


# %%
def create_spark_session():
    return (
        SparkSession.builder.appName("Neo4jIntegration")
        .config(
            "spark.jars",
            jar_path,
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
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
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
def write_nodes(df, label, key_cols, mode="Overwrite"):
    (
        df.write.format("org.neo4j.spark.DataSource")
        .mode(mode)
        .option("labels", f":{label}")
        .option("node.keys", ",".join(key_cols))
        .save()
    )


# %%
def write_rels(
    df,
    rel_type,
    src_label,
    src_key,
    src_col,
    tgt_label,
    tgt_key,
    tgt_col,
    prop_cols=None,
    mode="Overwrite",
):
    w = (
        df.write.format("org.neo4j.spark.DataSource")
        .mode(mode)
        .option("relationship", rel_type)
        .option("relationship.save.strategy", "keys")
        .option("relationship.source.labels", f":{src_label}")
        .option("relationship.source.node.keys", f"{src_col}:{src_key}")
        .option("relationship.target.labels", f":{tgt_label}")
        .option("relationship.target.node.keys", f"{tgt_col}:{tgt_key}")
    )
    if prop_cols:
        w = w.option("relationship.properties", ",".join(prop_cols))
    w.save()


# %%
def clean(colname: str):
    no_web = regexp_replace(col(colname), r"\.(com|net|org|io)$", "")
    return initcap(
        trim(
            regexp_replace(
                regexp_replace(
                    regexp_replace(no_web, r"\(.*?\)", " "),
                    r",.*$",
                    "",
                ),
                r"\s+",
                " ",
            )
        )
    )


# %%
from pyspark.sql.functions import col, regexp_replace, trim, initcap


def clean_name(colname: str):
    no_titles = regexp_replace(col(colname), r",\s*(M?Sc\.?|B?Sc\.?|PhD\.?|MBA\.?)", "")
    no_parens = regexp_replace(no_titles, r"\(.*?\)", " ")
    no_loose_dots = regexp_replace(no_parens, r"\s*\.(?=\S)", "")
    single_space = trim(regexp_replace(no_loose_dots, r"\s+", " "))
    return initcap(single_space)


# %%
"""def clear_db():

driver = GraphDatabase.driver(url, auth=(username, password))
with driver.session() as session:
    session.run("MATCH (n) DETACH DELETE n")
driver.close()
"""

# clear_db()
# print("Database cleared")

# %%
base_path = "../../data/"

# %%
positions_df = (
    spark.read.option("header", True)
    .csv(base_path + "Positions.csv")
    .withColumnRenamed("Company Name", "company")
    .withColumn("from", F.to_date(F.col("Started On"), "MMM yyyy").cast(DateType()))
    .withColumn("to", F.to_date(F.col("Finished On"), "MMM yyyy").cast(DateType()))
    .withColumn("user", F.lit(me))
    .select(
        "user",
        "company",
        F.col("Title").alias("title"),
        F.col("Location").alias("location"),
        "from",
        "to",
    )
    .dropDuplicates()
)

# %%
education_df = (
    spark.read.option("header", True)
    .csv(base_path + "Education.csv")
    .withColumnRenamed("School Name", "school")
    .withColumn("from", F.to_date(F.col("Start Date"), "MMM yyyy").cast(DateType()))
    .withColumn("to", F.to_date(F.col("End Date"), "MMM yyyy").cast(DateType()))
    .withColumn("user", F.lit(me))
    .select("user", "school", F.col("Degree Name").alias("degree"), "from", "to")
    .dropDuplicates()
)
universities = (
    education_df.select("school").distinct().withColumnRenamed("school", "name")
)

# %%
langs_df = (
    spark.read.option("header", True)
    .csv(base_path + "Languages.csv")
    .withColumnRenamed("Name", "name")
    .withColumnRenamed("Proficiency", "proficiency")
    .withColumn("name", F.initcap("name"))
    .select("name", "proficiency")
    .dropna(subset=["name"])
    .dropDuplicates(["name", "proficiency"])
)


user_langs_df = langs_df.withColumn("user", F.lit(me)).select(
    F.col("user"), F.col("name"), F.col("proficiency")
)

# %%
raw_certs_df = (
    spark.read.option("header", True)
    .csv(base_path + "Certifications.csv")
    .withColumnRenamed("Name", "name")
    .withColumnRenamed("Authority", "authority")
    .withColumn("name", F.trim(F.col("name")))
    .withColumn("authority", F.trim(F.col("authority")))
    .withColumn("start", F.to_date(F.col("Started On"), "MMM yyyy").cast(DateType()))
    .withColumn("license", F.col("License Number"))
    .dropDuplicates(["name", "authority"])
)

certs_df = raw_certs_df.select("name", "authority").dropDuplicates(
    ["name", "authority"]
)
user_certs_df = (
    raw_certs_df.withColumn("user", F.lit(me))
    .select("user", "name", "authority", "start", "license")
    .dropDuplicates(["user", "name", "authority", "start", "license"])
)

# %%
raw_jobs_df = (
    spark.read.option("header", True)
    .csv(base_path + "Saved Jobs.csv")
    .withColumnRenamed("Job Title", "title")
    .withColumnRenamed("Company Name", "company")
    .withColumn("id", F.monotonically_increasing_id())
    .withColumn("savedDate", F.to_timestamp(F.col("Saved Date"), "M/d/yy, h:mm a"))
    .withColumn("title", F.trim(F.col("title")))
    .withColumn("company", F.trim(F.col("company")))
)

jobs_df = raw_jobs_df.select("id", "title", "company").dropDuplicates(["id"])

user_saved_df = raw_jobs_df.select(
    F.lit(me).alias("user"), "id", "savedDate"
).dropDuplicates(["user", "id", "savedDate"])

# %%
skills_df = (
    spark.read.option("header", True)
    .csv(base_path + "Skills.csv")
    .withColumn("name", F.initcap(F.col("Name")))
    .withColumn("nameLower", F.lower(F.col("name")))
    .select("name", "nameLower")
    .dropna(subset=["name"])
    .dropDuplicates(["nameLower"])
)

user_skills_df = (
    spark.read.option("header", True)
    .csv(base_path + "Skills.csv")
    .withColumn("user", F.lit(me))
    .withColumn("skill", F.initcap(F.col("Name")))
    .select(F.col("user"), F.col("skill"))
    .dropDuplicates()
)

# %%
follows_df = (
    spark.read.option("header", True)
    .csv(base_path + "Company Follows.csv")
    .withColumnRenamed("Organization", "company")
    .withColumn(
        "since", F.to_timestamp(F.col("Followed On"), "EEE MMM dd HH:mm:ss z yyyy")
    )
    .withColumn("user", F.lit(me))
    .withColumn("company", clean("company"))
    .select("user", "company", "since")
    .dropDuplicates()
)

# %%
raw = spark.read.option("header", True).csv(base_path + "Connections.csv")

connections_work_df = (
    raw.withColumn("raw_name", concat_ws(" ", col("First Name"), col("Last Name")))
    .withColumn("user", clean("raw_name"))
    .withColumn("company", clean("Company"))
    .withColumn("title", trim(col("Position")))
    .select("user", "company", "title")
    .dropna(subset=["user", "company"])
    .dropDuplicates()
)

# %%
inv_df = (
    spark.read.option("header", True)
    .csv(base_path + "Invitations.csv")
    .withColumnRenamed("From", "fromName")
    .withColumnRenamed("To", "toName")
    .withColumn("date", F.to_timestamp(F.col("Sent At"), "M/d/yy, h:mm a"))
    .withColumnRenamed("Direction", "direction")
    .select("fromName", "toName", "date", "direction")
    .dropDuplicates()
)
invitation_users_df = (
    inv_df.select(F.col("fromName").alias("name"))
    .union(inv_df.select(F.col("toName").alias("name")))
    .distinct()
    .sort("name")
)

# %%
companies = (
    positions_df.select("company")
    .union(jobs_df.select("company"))
    .union(follows_df.select("company"))
    .distinct()
    .withColumnRenamed("company", "name")
)

# %%
raw = spark.read.option("header", True).csv(base_path + "Connections.csv")

connections_users_df = (
    raw.withColumn(
        "raw_name", concat_ws(" ", trim(col("First Name")), trim(col("Last Name")))
    )
    .withColumn("name", clean_name("raw_name"))
    .select("name")
    .dropna()
    .dropDuplicates()
    .filter(col("name") != "")
)

# %%
users_from_positions = positions_df.select(F.col("user").alias("name")).distinct()
all_users_df = (
    users_from_positions.union(connections_users_df)
    .union(invitation_users_df)
    .withColumn("name", clean("name"))
    .distinct()
    .withColumn("name", initcap(trim(regexp_replace(col("name"), ",.*$-", ""))))
    .filter(col("name").isNotNull() & (col("name") != ""))
)

# %%
raw = spark.read.option("header", True).csv(base_path + "Connections.csv")

conns_df = (
    raw.withColumn("nameA", concat_ws(" ", col("First Name"), col("Last Name")))
    .withColumn("nameA", clean_name("nameA"))
    .withColumn("nameB", F.lit(me))
    .withColumn("since", F.to_date(trim(col("Connected On")), "dd MMM yyyy"))
    .select("nameA", "nameB", "since")
    .dropna(subset=["nameA"])
    .dropDuplicates(["nameA", "nameB", "since"])
    .filter(col("nameA").isNotNull() & (col("nameA") != ""))
    .filter(col("nameB").isNotNull() & (col("nameB") != ""))
    .filter(col("nameA") != "")
    .filter(col("since").isNotNull())
)

# %%
companies_df = (
    positions_df.select(F.col("company").alias("name"))
    .union(connections_work_df.select(F.col("company").alias("name")))
    .union(follows_df.select(F.col("company").alias("name")))
    .union(jobs_df.select(F.col("Company").alias("name")))
    .withColumn("name", clean("name"))
    .distinct()
    .filter(col("name") != "")
)

# %%
education_df = (
    spark.read.option("header", True)
    .csv(base_path + "Education.csv")
    .withColumnRenamed("School Name", "school")
    .withColumn("from", F.to_date(F.col("Start Date"), "MMM yyyy").cast(DateType()))
    .withColumn("to", F.to_date(F.col("End Date"), "MMM yyyy").cast(DateType()))
    .withColumn("user", F.lit(me))
    .select("user", "school", F.col("Degree Name").alias("degree"), "from", "to")
    .dropDuplicates()
)

universities = (
    education_df.select("school").distinct().withColumnRenamed("school", "name")
)

# %%
write_nodes(all_users_df, "User", ["name"])
print(f"Writting {all_users_df.count()} USER nodes")

# %%
write_nodes(companies_df, "Company", ["name"])
print(f"Writting {companies_df.count()} COMPANY nodes")

# %%
write_nodes(universities, "University", ["name"])
print(f"Writting {universities.count()} UNIVERSITY nodes")

# %%
write_nodes(langs_df.select("name"), "Language", ["name"])
print(f"Writting {langs_df.count()} LANGUAGE nodes")

# %%
write_nodes(
    raw_certs_df.select("name", "authority"), "Certification", ["name", "authority"]
)
print(f"Writting {raw_certs_df.count()} CERTIFICATION nodes")

# %%
write_nodes(skills_df.select("name"), "Skill", ["name"])
print(f"Writting {skills_df.count()} SKILL nodes")

# %%
jobs_nodes_df = jobs_df.select("id", "title", "company").dropDuplicates(["id"])
write_nodes(jobs_nodes_df, "Job", ["id"])
print(f"Writting {jobs_nodes_df.count()} JOB nodes")

# %%
write_rels(
    education_df.withColumn("user", lit(me)).select(
        "user", "school", "degree", "from", "to"
    ),
    "STUDIED_AT",
    "User",
    "name",
    "user",
    "University",
    "name",
    "school",
    prop_cols=["degree", "from", "to"],
)
print(f"Writting {education_df.count()} STUDIED_AT relationships")

# %%
write_rels(
    langs_df.withColumn("user", lit(me)).select("user", "name", "proficiency"),
    "SPEAKS",
    "User",
    "name",
    "user",
    "Language",
    "name",
    "name",
    prop_cols=["proficiency"],
)
print(f"Writting {langs_df.count()} SPEAKS relationships")

# %%
write_rels(
    raw_certs_df.withColumn("user", lit(me)).select(
        "user", "name", "authority", "start", "license"
    ),
    "HAS_CERT",
    "User",
    "name",
    "user",
    "Certification",
    "name",
    "name",
    prop_cols=["start", "license"],
)
print(f"Writting {raw_certs_df.count()} HAS_CERT relationships")

# %%
write_rels(
    skills_df.withColumn("user", lit(me)).select("user", "name"),
    "HAS_SKILL",
    "User",
    "name",
    "user",
    "Skill",
    "name",
    "name",
)
print(f"Writting {skills_df.count()} HAS_SKILL relationships")

# %%
write_rels(
    positions_df.withColumn("user", lit(me)).select(
        "user", "company", "title", "location", "from", "to"
    ),
    "WORKED_AT",
    "User",
    "name",
    "user",
    "Company",
    "name",
    "company",
    prop_cols=["title", "location", "from", "to"],
)
print(f"Writting {positions_df.count()} WORKED_AT relationships")
write_rels(
    connections_work_df,
    "WORKED_AT",
    "User",
    "name",
    "user",
    "Company",
    "name",
    "company",
    prop_cols=["title"],
)
print(f"Writting {connections_work_df.count()} WORKED_AT relationships")

# %%
write_rels(
    inv_df.select("fromName", "toName", "date", "direction"),
    "INVITED",
    "User",
    "name",
    "fromName",
    "User",
    "name",
    "toName",
    prop_cols=["date", "direction"],
)
print(f"Writting {inv_df.count()} INVITED relationships")

# %%
write_rels(
    conns_df,
    "CONNECTED",
    "User",
    "name",
    "nameA",
    "User",
    "name",
    "nameB",
    prop_cols=["since"],
)
print(f"Writting {conns_df.count()} CONNECTED relationships")

# %%
write_rels(
    follows_df,
    "FOLLOWS",
    "User",
    "name",
    "user",
    "Company",
    "name",
    "company",
    prop_cols=["since"],
)
print(f"Writting {follows_df.count()} FOLLOWS relationships")

# %%
write_rels(
    user_saved_df,
    "SAVED_JOB",
    "User",
    "name",
    "user",
    "Job",
    "id",
    "id",
    prop_cols=["savedDate"],
)
print(f"Writing {user_saved_df.count()} SAVED_JOB relationships")

# %%
neo_connections = (
    spark.read.format("org.neo4j.spark.DataSource")
    .option("relationship", "CONNECTED")
    .option("relationship.source.labels", ":User")
    .option("relationship.target.labels", ":User")
    .option("relationship.source.node.keys", "name:name")
    .option("relationship.target.node.keys", "name:name")
    .load()
    .selectExpr(
        "`source.name` AS nameA", "`target.name` AS nameB", "`rel.since` AS since"
    )
)

csv_connections = (
    spark.read.option("header", True)
    .csv(base_path + "Connections.csv")
    .withColumn("nameA", concat_ws(" ", col("First Name"), col("Last Name")))
    .withColumn("nameB", lit(me))
    .withColumnRenamed("Connected On", "since")
    .withColumn("since", to_date("since", "dd MMM yyyy"))
    .select("nameA", "nameB", "since")
    .dropDuplicates(["nameA", "nameB", "since"])
)

missing_connections = csv_connections.join(
    neo_connections, on=["nameA", "nameB", "since"], how="left_anti"
)
print(f"Missing connections in Neo4j: {missing_connections.count()}")
missing_connections.show(500, truncate=False)

csv_users = (
    spark.read.option("header", True)
    .csv(base_path + "Connections.csv")
    .withColumn("name", concat_ws(" ", col("First Name"), col("Last Name")))
    .select("name")
    .distinct()
)

neo_users = (
    spark.read.format("org.neo4j.spark.DataSource")
    .option("labels", ":User")
    .option("node.keys", "name")
    .load()
    .select("name")
)

missing_users = csv_users.join(neo_users, on="name", how="left_anti")
print(f"Missing users in Neo4j: {missing_users.count()}")
missing_users.show(truncate=False)

neo_companies = (
    spark.read.format("org.neo4j.spark.DataSource")
    .option("labels", ":Company")
    .option("node.keys", "name")
    .load()
    .select("name")
)

missing_companies = companies_df.join(neo_companies, on="name", how="left_anti")
print(f"Missing companies in Neo4j: {missing_companies.count()}")
missing_companies.show(truncate=False)


# %%
def check_duplicates():
    driver = GraphDatabase.driver(url, auth=(username, password))

    queries = [
        """
        MATCH (n:User)
        WITH n.name AS name, COUNT(*) AS count
        WHERE count > 1
        RETURN name, count
        """,
        """
        MATCH (n:Company)
        WITH n.name AS name, COUNT(*) AS count
        WHERE count > 1
        RETURN name, count
        """,
        """
        MATCH (n:University)
        WITH n.name AS name, COUNT(*) AS count
        WHERE count > 1
        RETURN name, count
        """,
        """
        MATCH (n:Skill)
        WITH n.name AS name, COUNT(*) AS count
        WHERE count > 1
        RETURN name, count
        """,
    ]

    try:
        with driver.session() as session:
            for query in queries:
                result = session.run(query)
                print("\nResults:")
                for record in result:
                    print(f"Nombre: {record['name']}, Conteo: {record['count']}")
    finally:
        driver.close()


check_duplicates()

# %%
