import findspark
findspark.init()
import pyspark
from SparkInitializer import SparkInitializer
import datetime

#create a spark session
spark = SparkInitializer("local", "insert")

#create all needed datframes
authors = spark.createCustomizedDataFrame("authors", verbose=False)
articles = spark.createCustomizedDataFrame("articles", verbose=False)
books = spark.createCustomizedDataFrame("books", verbose=False)
composed_by = spark.createCustomizedDataFrame("composed_by", verbose=False)

#INSERT 1:
print("Add author Marco Brambilla to authors dataframe")
newRow = [("Marco Brambilla", "marco.brambilla@polimi.it", "Politecnico di Milano")]
newRow = spark.getSparkSession().createDataFrame(data=newRow, schema=spark.getAuthorSchema())
authors = authors.union(newRow)
authors.filter(authors.name=="Marco Brambilla").show(truncate=False)

#INSERT 2:
print("Add article \"The Role of Human Knowledge in Explainable AI\" to articles dataframe")
newRow = [("10.3390/data7070093", "The Role of Human Knowledge in Explainable AI", datetime.date(2022,10,22), 2022, "October", "",
           "https://doi.org/10.3390/data7070093", "", 7, 7, "93", "Data")]
newRow = spark.getSparkSession().createDataFrame(data=newRow, schema=spark.getArticleSchema())
articles = articles.union(newRow)
articles.filter(articles.key == "10.3390/data7070093").show(truncate=False)

#INSERT 3:
print("Add a new book to book dataframe")
newRow = [("books/sp/AntoniouT23", "Effects of Data Overload on User Quality of Experience", datetime.date(2022,8,3), 2022,
           "978-3-031-06869-0", "https://doi.org/10.1007/978-3-031-06870-6", "", "Springer")]
newRow = spark.getSparkSession().createDataFrame(data=newRow, schema=spark.getBookSchema())
books = books.union(newRow)
books.filter(books.key=="books/sp/AntoniouT23").show(truncate=False)

#INSERT 4:
print("Add new relationship between article and author in composedBy dataframe")
newRow = [("10.3390/data7070093", "Marco Brambilla")]
newRow = spark.getSparkSession().createDataFrame(data=newRow, schema=spark.getComposedBySchema())
composed_by = composed_by.union(newRow)
composed_by.filter((composed_by.articleKey=="10.3390/data7070093") & (composed_by.author=="Marco Brambilla")).show(truncate=False)

#CREATION OF A SUB-DATAFRAME:
print("Creation of sub-dataframe of all article published on ACM Crossroads")
ACM_articles = articles.filter(articles.journal == "ACM Crossroads")
ACM_articles.show()

#DELETE:
print("Delete all the article written before 1975")
articles = articles.filter(articles.year>1974)
articles.filter(articles.year<1975).show(truncate=False)
