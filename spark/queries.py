import findspark
findspark.init()
import pyspark
from pyspark.sql.functions import col, count, max, avg, year
from pyspark.sql.types import IntegerType
from SparkInitializer import SparkInitializer

#create a spark session
spark = SparkInitializer("local", "queries")

#create all needed datframes
articles = spark.createCustomizedDataFrame("articles", verbose=False)
proceedings = spark.createCustomizedDataFrame("proceedings", verbose=False)
publishers = spark.createCustomizedDataFrame("publishers", verbose=False)
editors = spark.createCustomizedDataFrame("editors", verbose=False)
revised_by = spark.createCustomizedDataFrame("revised_by", verbose=False)
inproceedings = spark.createCustomizedDataFrame("inproceedings", verbose=False)
authors = spark.createCustomizedDataFrame("authors", verbose=False)
incollections = spark.createCustomizedDataFrame("incollections", verbose=False)
composed_by = spark.createCustomizedDataFrame("composed_by", verbose=False)
books = spark.createCustomizedDataFrame("books", verbose=False)
edited_by = spark.createCustomizedDataFrame("edited_by", verbose=False)


#QUERY NO. 1:
print("Proceedings that have been issued by publishers whose headquarter is in Berlin or Munich")
proceedings.join(publishers, proceedings.publisher == publishers.name, "left") \
        .filter((publishers.headquarter == "Berlin") | (publishers.headquarter == "Munich")) \
        .select(col("key"), col("publisher"), col("headquarter"))\
        .show(truncate=False)

#QUERY NO. 2:
print("The first 5 articles’ titles which contain \"comput\" inside")
articles.filter(articles.title.like("%comput%"))\
	    .limit(5)\
        .select(col("title"), col("url"))\
	    .show(truncate=False)

#QUERY NO. 3:
print("All Brazilian editors that did not revise a proceeding")
editors.select(editors['*'])\
        .filter(~col('name').isin(revised_by.select(col('editor'))\
        .rdd.flatMap(lambda x: x).collect()) & (col('country') == 'Brazil')).show()


#QUERY NO. 4:
print("Number of inproceedings and the max year in which an associated inproceeding has been pubished for each proceeding")
inproceedings.withColumnRenamed("year", "inproceedingYear")\
    .join(proceedings, inproceedings.proceedingKey == proceedings.key, "inner")\
    .drop(proceedings.key)\
    .groupBy("proceedingKey")\
    .agg(count("proceedingKey").alias("number_inproceedings"), max(col('inproceedingYear')).alias("max_year"))\
    .show(truncate=False)

#QUERY NO. 5:
print("Number of authors for each of the top 5 universities")
top_5_universities = ["Massachusetts Institute of Technology",
                        "University of Oxford",
                        "Stanford University",
                        "University of Cambridge",
                        "Harvard University"
                      ]
authors.filter(authors.affiliation.isin(top_5_universities))\
        .groupBy("affiliation")\
        .agg(count("affiliation"))\
        .show(truncate=False)

#QUERY NO. 6:
print("Number of articles of the journals that have maximum volume greater than 20")
articles.groupBy("journal")\
	    .agg(count("title").alias("number_articles"), max("volume").alias("max_volume"))\
	    .filter(col("max_volume") >= 20)\
	    .orderBy(col("max_volume").desc())\
	    .show(truncate=False)

#QUERY NO. 7:
print("Books having at least 50 incollections associated with field “ee” not null")
incollections.filter(col("ee").isNotNull())\
	            .groupBy(col("bookKey"))\
                .agg(count(col("title")).alias("Number of Incollections"))\
	            .filter(col("Number of Incollections")>50)\
	            .show(truncate = False)

#QUERY NO. 8:
print("Authors whose name starts with “A” and have written more articles than the average")
composed_by.filter(col("author").startswith("A"))\
	        .groupBy(col("author")).agg(count(col("articleKey")).alias("Number of articles"))\
	        .filter(col("Number of articles") > composed_by.groupBy(col("author")).agg(count(col("articleKey")).alias("num"))\
	        .select(avg("num")).collect()[0][0]).show(truncate = False)

#QUERY NO. 9:
print("Last date a book has been modified, with modification year higher or equal than 2011,"
      "for each year in which a book has been published by any publisher located in London")
books.join(publishers, publishers.name == books.publisher, 'right')\
        .filter(publishers.headquarter == 'London')\
        .groupBy(books.year)\
        .agg(max(books.mdate).alias('most recent mdate'))\
        .orderBy(col('year'))\
        .filter(year(col('most recent mdate')) >= 2011)\
        .show(truncate=False)

#QUERY NO. 10:
print("Austrian editors that have written at least 2 books after the 2007")
edited_by.join(books, books.key == edited_by.bookKey, "full")\
            .join(editors, edited_by.editor == editors.name, "full")\
            .filter((editors.country == "Austria") & (books.year > 2007))\
            .groupBy("name")\
            .agg(count ("name").alias("publications"))\
            .filter(col("publications") > 1)\
            .sort("publications", ascending= False)\
            .show(truncate=False)