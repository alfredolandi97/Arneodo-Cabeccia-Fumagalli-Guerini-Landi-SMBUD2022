import findspark
findspark.init()
import pyspark
from SparkInitializer import SparkInitializer

spark = SparkInitializer("local", "insert")


#INSERT 1: add author Marco Brambilla to authors dataframe
df_author = spark.createCustomizedDataFrame("authors")
newRow = [("Marco Brambilla", "marco.brambilla@polimi.it", "Politecnico di Milano")]
df_newRow = spark.getSparkSession().createDataFrame(data=newRow, schema=spark.getAuthorSchema())
df_author = df_author.union(df_newRow)

#INSERT 2: add article "The Role of Human Knowledge in Explainable AI" to articles dataframe
import datetime
df_article = spark.createCustomizedDataFrame("articles")
newRow = [("10.3390/data7070093", "The Role of Human Knowledge in Explainable AI", datetime.date(2022,10,22), 2022, "October", "",
           "https://doi.org/10.3390/data7070093", "", 7, 7, "93", "Data")]
df_newRow = spark.getSparkSession().createDataFrame(data=newRow, schema=spark.getArticleSchema())
df_article = df_article.union(df_newRow)

#INSERT 3: add a new book to book dataframe
df_book = spark.createCustomizedDataFrame("books")
newRow = [("books/sp/AntoniouT23", "Effects of Data Overload on User Quality of Experience", datetime.date(2022,8,3), 2022,
           "978-3-031-06869-0", "https://doi.org/10.1007/978-3-031-06870-6", "", "Springer")]
df_newRow = spark.getSparkSession().createDataFrame(data=newRow, schema=spark.getBookSchema())
df_book = df_book.union(df_newRow)

#INSERT 4: add new relationship between article and author in composedBy dataframe
df_composedBy = spark.createCustomizedDataFrame("composedBy")
newRow = [("10.3390/data7070093", "Marco Brambilla")]
df_newRow = spark.getSparkSession().createDataFrame(data=newRow, schema=spark.getComposedBySchema())
df_composedBy = df_composedBy.union(df_newRow)

#DELETE: delete all the article written before 1975
df_article = df_article.filter(df_article.year>1974)

#CREATION OF A SUB-DATAFRAME: creation of sub-dataframe of all article published on Wikipedia
wikipedia_df_article = df_article.filter(df_article.journal == "ACM Crossroads")
wikipedia_df_article.show()
