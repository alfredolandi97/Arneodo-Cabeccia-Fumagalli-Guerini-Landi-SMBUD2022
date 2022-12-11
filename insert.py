import findspark
findspark.init()
import pyspark
from SparkInitializer import SparkInitializer

spark = SparkInitializer("local", "insert")


#INSERT 1: add author Marco Brambilla to authors dataframe
authors = spark.createCustomizedDataFrame("authors", verbose=False)
newRow = [("Marco Brambilla", "marco.brambilla@polimi.it", "Politecnico di Milano")]
newRow = spark.getSparkSession().createDataFrame(data=newRow, schema=spark.getAuthorSchema())
authors = authors.union(newRow)

#INSERT 2: add article "The Role of Human Knowledge in Explainable AI" to articles dataframe
import datetime
articles = spark.createCustomizedDataFrame("articles", verbose=False)
newRow = [("10.3390/data7070093", "The Role of Human Knowledge in Explainable AI", datetime.date(2022,10,22), 2022, "October", "",
           "https://doi.org/10.3390/data7070093", "", 7, 7, "93", "Data")]
newRow = spark.getSparkSession().createDataFrame(data=newRow, schema=spark.getArticleSchema())
articles = articles.union(newRow)

#INSERT 3: add a new book to book dataframe
books = spark.createCustomizedDataFrame("books", verbose=False)
newRow = [("books/sp/AntoniouT23", "Effects of Data Overload on User Quality of Experience", datetime.date(2022,8,3), 2022,
           "978-3-031-06869-0", "https://doi.org/10.1007/978-3-031-06870-6", "", "Springer")]
newRow = spark.getSparkSession().createDataFrame(data=newRow, schema=spark.getBookSchema())
books = books.union(newRow)

#INSERT 4: add new relationship between article and author in composedBy dataframe
composed_by = spark.createCustomizedDataFrame("composed_by", verbose=False)
newRow = [("10.3390/data7070093", "Marco Brambilla")]
newRow = spark.getSparkSession().createDataFrame(data=newRow, schema=spark.getComposedBySchema())
composed_by = composed_by.union(newRow)

#DELETE: delete all the article written before 1975
#articles = articles.filter(articles.year>1974)

#CREATION OF A SUB-DATAFRAME: creation of sub-dataframe of all article published on ACM Crossroads
wikipedia_articles = articles.filter(articles.journal == "ACM Crossroads")
wikipedia_articles.show()
