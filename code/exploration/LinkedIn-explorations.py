# %%
# %pip install pyspark pyarrow pandas

# %%
from pyspark.sql import SparkSession

try:
    spark
except NameError:
    print("🔄 No existing SparkSession found. Creating a new one...")
    spark = SparkSession.builder.appName("Tu App").master("local[*]").getOrCreate()
    print("✅ SparkSession created successfully.")
else:
    if spark._jsparkSession is None:
        print("⚠️ Existing SparkSession is not active. Recreating it...")
        spark = SparkSession.builder.appName("Tu App").master("local[*]").getOrCreate()
        print("✅ SparkSession re-created successfully.")
    else:
        print("✅ SparkSession is already active.")

print(f"🔥 Spark is running — version: {spark.version}")

# %%
base_path = "../../data/"

# %%
skills_df = spark.read.csv(base_path + "Skills.csv", header=True, inferSchema=True)
print("Schema: ")
skills_df.printSchema()
count_skills = skills_df.count()
print(f"Number of skills: {count_skills}")
print("\nFirst rows: ")
skills_df.show(5)

# %%
profile_df = spark.read.csv(base_path + "Profile.csv", header=True, inferSchema=True)
print("Schema: ")
profile_df.printSchema()
count_profile = profile_df.count()
print(f"Number of profile: {count_profile}")
print("\nFirst rows: ")
profile_df.show(5)

# %%
positions_df = spark.read.csv(
    base_path + "Positions.csv", header=True, inferSchema=True
)
print("Schema: ")
positions_df.printSchema()
count_positions = positions_df.count()
print(f"Number of positions: {count_positions}")
print("\nFirst rows: ")
positions_df.show(5)

# %%
languages_df = spark.read.csv(
    base_path + "Languages.csv", header=True, inferSchema=True
)
print("Schema: ")
languages_df.printSchema()
count_languages = languages_df.count()
print(f"Number of languages: {count_languages}")
print("\nFirst rows: ")
languages_df.show(5)

# %%
invitations_df = spark.read.csv(
    base_path + "Invitations.csv", header=True, inferSchema=True
)
print("Schema: ")
invitations_df.printSchema()
count_invitations = invitations_df.count()
print(f"Number of invitations: {count_invitations}")
print("\nFirst rows: ")
invitations_df.show(5)

# %%
education_df = spark.read.csv(
    base_path + "Education.csv", header=True, inferSchema=True
)
print("Schema: ")
education_df.printSchema()
count_education = education_df.count()
print(f"Number of education: {count_education}")
print("\nFirst rows: ")
education_df.show(5)

# %%
connections_df = spark.read.csv(
    base_path + "Connections.csv", header=True, inferSchema=True
)
print("Schema: ")
connections_df.printSchema()
count_connections = connections_df.count()
print(f"Number of connections: {count_connections}")
print("\nFirst rows: ")
connections_df.show(5)

# %%
company_followers_df = spark.read.csv(
    base_path + "Company Follows.csv", header=True, inferSchema=True
)
print("Schema: ")
company_followers_df.printSchema()
count_company_followers = company_followers_df.count()
print(f"Number of company followers: {count_company_followers}")
print("\nFirst rows: ")
company_followers_df.show(5)

# %%
certifications_df = spark.read.csv(
    base_path + "Certifications.csv", header=True, inferSchema=True
)
print("Schema: ")
certifications_df.printSchema()
count_certifications = certifications_df.count()
print(f"Number of certifications: {count_certifications}")
print("\nFirst rows: ")
certifications_df.show(5)

# %%
saved_jobs_df = spark.read.csv(
    base_path + "Saved Jobs.csv", header=True, inferSchema=True
)
print("Schema: ")
saved_jobs_df.printSchema()
count_saved_jobs = saved_jobs_df.count()
print(f"Number of saved jobs: {count_saved_jobs}")
print("\nFirst rows: ")
saved_jobs_df.show(5)

# %%
