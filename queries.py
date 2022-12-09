import findspark
findspark.init()
import pyspark
from SparkInitializer import SparkInitializer

#create a spark session
spark = SparkInitializer("local", "queries")
df_article = spark.createCustomizedDataFrame("articles")
#query 2: show the first 5 results about article's titles which contains "comput" inside
df_article.filter(df_article.title.like("%comput%")).limit(5).show(truncate=False)
