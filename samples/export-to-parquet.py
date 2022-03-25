from pyspark.sql import SparkSession

tenantId = ""
clientId = ""
clientSecret = ""
partitionId = ""
osduApiEndpoint = "https://"
oauthEndpoint = f"https://login.microsoftonline.com/{tenantId}/oauth2/token"

spark = (SparkSession.builder
    .master("local[1]")
    .config('spark.jars', "../spark-datasource/target/scala-2.12/osdu-spark-connector_2.12-1.0.jar,/home/marcozo/.m2/repository/io/github/nur858/com-microsoft-osdu-api/0.0.4/com-microsoft-osdu-api-0.0.4.jar")
    .appName("OSDUSparkExample")
    .getOrCreate())

df = (spark.read
        .format("com.microsoft.spark.osdu")
        .option("clientId", clientId)
        .option("clientSecret", clientSecret)
        .option("oauthEndpoint", oauthEndpoint)
        .option("kind", "osdu:wks:master-data--GeoPoliticalEntity:1.0.0")
        .option("query", "*")
        .option("osduApiEndpoint", osduApiEndpoint)
        .option("partitionId", partitionId)
        .load())

df.show()

df.write.parquet("sample.parquet")