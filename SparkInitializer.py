import findspark
findspark.init()
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType
import csv
import datetime


class SparkInitializer:
    spark = None

    def __init__(self, master, appName):
        self.spark = SparkSession.builder.master(master).appName(appName).getOrCreate()


    def createCustomizedDataFrame(self, dataFrame):
        if dataFrame == "articles":
            self.schemaArticle = StructType([
                StructField("key", StringType(), False),
                StructField("title", StringType(), True),
                StructField("mdate", DateType(), True),
                StructField("year", IntegerType(), True),
                StructField("month", StringType(), True),
                StructField("cdrom", StringType(), True),
                StructField("url", StringType(), True),
                StructField("ee", StringType(), True),
                StructField("number", IntegerType(), True),
                StructField("volume", IntegerType(), True),
                StructField("pages", StringType(), True),
                StructField("journal", StringType(), False)
            ])

            articles_csv = open("./resources/articles.csv", newline="", encoding="utf-8")
            articles_reader = csv.reader(articles_csv, delimiter=";")

            # data parsing...
            df_data_articles = []
            for row in articles_reader:
                row[2] = datetime.datetime.strptime(row[2], '%Y-%m-%d')
                row[3] = int(row[3])
                try:
                    row[8] = int(row[8])
                except:
                    row[8] = None
                try:
                    row[9] = int(row[9])
                except:
                    row[9] = None
                df_data_articles.append(row)

            df_article = self.spark.createDataFrame(data=df_data_articles, schema=self.schemaArticle)
            df_article.printSchema()
            df_article.show()
            return df_article
        elif dataFrame=="authors":
            self.schemaAuthor = StructType([
                StructField("name", StringType(), False),
                StructField("email", StringType(), True),
                StructField("affiliation", StringType(), True)
            ])

            authors_csv = open("./resources/authors.csv", newline="", encoding="utf-8")
            df_data_authors = csv.reader(authors_csv, delimiter=";")

            df_author = self.spark.createDataFrame(data = df_data_authors, schema=self.schemaAuthor)
            df_author.printSchema()
            df_author.show()
            return df_author
        elif dataFrame=="books":
            self.schemaBook = StructType([
                StructField("key", StringType(), False),
                StructField("title", StringType(), True),
                StructField("mdate", DateType(), True),
                StructField("year", IntegerType(), True),
                StructField("isbn", StringType(), True),
                StructField("url", StringType(), True),
                StructField("ee", StringType(), True),
                StructField("publisher", StringType(), False)
            ])

            books_csv = open("./resources/books.csv", newline="", encoding="utf-8")
            books_reader = csv.reader(books_csv, delimiter=";")

            df_data_books = []
            for row in books_reader:
                row[2] = datetime.datetime.strptime(row[2], '%Y-%m-%d')
                row[3] = int(row[3])

                df_data_books.append(row)

            df_book = self.spark.createDataFrame(data=df_data_books, schema=self.schemaBook)
            df_book.printSchema()
            df_book.show()
            return df_book
        elif dataFrame=="composedBy":
            self.schemaComposedBy = StructType([
                StructField("articleKey", StringType(), False),
                StructField("author", StringType(), False),
            ])

            composedBy_csv = open("./resources/composed_by.csv", newline="", encoding="utf-8")
            df_data_composedBy = csv.reader(composedBy_csv, delimiter=";")

            df_composedBy = self.spark.createDataFrame(data=df_data_composedBy, schema=self.schemaComposedBy)
            df_composedBy.printSchema()
            df_composedBy.show()
            return df_composedBy
        elif dataFrame=="editedBy":
            self.schemaEditedBy = StructType([
                StructField("articleKey", StringType(), False),
                StructField("editor", StringType(), False),
            ])

            editedBy_csv = open("./resources/edited_by.csv", newline="", encoding="utf-8")
            df_data_editedBy = csv.reader(editedBy_csv, delimiter=";")

            df_editedBy = self.spark.createDataFrame(data=df_data_editedBy, schema=self.schemaEditedBy)
            df_editedBy.printSchema()
            df_editedBy.show()
            return df_editedBy
        elif dataFrame=="editors":
            self.schemaEditor = StructType([
                StructField("name", StringType(), False),
                StructField("email", StringType(), True),
                StructField("country", StringType(), True)
            ])

            editors_csv = open("./resources/editors.csv", newline="", encoding="utf-8")
            df_data_editors = csv.reader(editors_csv, delimiter=";")

            df_editor = self.spark.createDataFrame(data=df_data_editors, schema=self.schemaEditor)
            df_editor.printSchema()
            df_editor.show()
            return df_editor
        elif dataFrame == "incollections":
            self.schemaIncollection = StructType([
                StructField("key", StringType(), False),
                StructField("title", StringType(), True),
                StructField("mdate", DateType(), True),
                StructField("year", IntegerType(), True),
                StructField("publType", StringType(), True),
                StructField("pages", StringType(), True),
                StructField("url", StringType(), True),
                StructField("ee", StringType(), True),
                StructField("bookKey", StringType(), False)
            ])

            incollections_csv = open("./resources/incollections.csv", newline="", encoding="utf-8")
            incollections_reader = csv.reader(incollections_csv, delimiter=";")

            df_data_incollections = []
            for row in incollections_reader:
                row[2] = datetime.datetime.strptime(row[2], '%Y-%m-%d')
                row[3] = int(row[3])

                df_data_incollections.append(row)

            df_incollection = self.spark.createDataFrame(data=df_data_incollections, schema=self.schemaIncollection)
            df_incollection.printSchema()
            df_incollection.show()
            return df_incollection
        elif dataFrame == "inproceedings":
            self.schemaInproceeding = StructType([
                StructField("key", StringType(), False),
                StructField("title", StringType(), True),
                StructField("mdate", DateType(), True),
                StructField("year", IntegerType(), True),
                StructField("pages", StringType(), True),
                StructField("url", StringType(), True),
                StructField("ee", StringType(), True),
                StructField("proceedingKey", StringType(), False)
            ])

            inproceedings_csv = open("./resources/inproceedings.csv", newline="", encoding="utf-8")
            inproceedings_reader = csv.reader(inproceedings_csv, delimiter=";")

            df_data_inproceedings = []
            for row in inproceedings_reader:
                row[2] = datetime.datetime.strptime(row[2], '%Y-%m-%d')
                row[3] = int(row[3])

                df_data_inproceedings.append(row)

            df_inproceedings = self.spark.createDataFrame(data=df_data_inproceedings, schema=self.schemaInproceeding)
            df_inproceedings.printSchema()
            df_inproceedings.show()
            return df_inproceedings
        elif dataFrame == "journals":
            self.schemaJournal = StructType([
                StructField("name", StringType(), False)
            ])

            journals_csv = open("./resources/journals.csv", newline="\n", encoding="ISO-8859-1")
            journals_reader = csv.reader(journals_csv, delimiter=";")

            #data parsing...
            df_data_books = []
            for row in journals_reader:
                record = []
                record.append(row[0])
                df_data_books.append(record)

            df_journal = self.spark.createDataFrame(data=df_data_books, schema=self.schemaJournal)
            df_journal.printSchema()
            df_journal.show()
            return df_journal
        elif dataFrame == "proceedings":
            self.schemaProceeding = StructType([
                StructField("key", StringType(), False),
                StructField("title", StringType(), True),
                StructField("mdate", DateType(), True),
                StructField("year", IntegerType(), True),
                StructField("series", StringType(), True),
                StructField("isbn", StringType(), True),
                StructField("url", StringType(), True),
                StructField("ee", StringType(), True),
                StructField("publisher", StringType(), False)
            ])

            proceedings_csv = open("./resources/proceedings.csv", newline="", encoding="utf-8")
            proceedings_reader = csv.reader(proceedings_csv, delimiter=";")

            df_data_proceedings = []
            for row in proceedings_reader:
                row[2] = datetime.datetime.strptime(row[2], '%Y-%m-%d')
                row[3] = int(row[3])

                df_data_proceedings.append(row)

            df_proceeding = self.spark.createDataFrame(data=df_data_proceedings, schema=self.schemaProceeding)
            df_proceeding.printSchema()
            df_proceeding.show()
            return df_proceeding
        elif dataFrame == "publishers":
            self.schemaPublisher = StructType([
                StructField("name", StringType(), False),
                StructField("headquarter", StringType(), True)
            ])

            publishers_csv = open("./resources/publishers.csv", newline="", encoding="utf-8")
            df_data_publishers = csv.reader(publishers_csv, delimiter=";")

            df_publisher = self.spark.createDataFrame(data=df_data_publishers, schema=self.schemaPublisher)
            df_publisher.printSchema()
            df_publisher.show()
            return df_publisher
        elif dataFrame == "revisedBy":
            self.schemaRevisedBy = StructType([
                StructField("proceedingKey", StringType(), False),
                StructField("editor", StringType(), False),
            ])

            revisedBy_csv = open("./resources/revised_by.csv", newline="", encoding="utf-8")
            df_data_revisedBy = csv.reader(revisedBy_csv, delimiter=";")

            df_revisedBy = self.spark.createDataFrame(data=df_data_revisedBy, schema=self.schemaRevisedBy)
            df_revisedBy.printSchema()
            df_revisedBy.show()
            return df_revisedBy
        elif dataFrame == "typedBy":
            self.schemaTypedBy = StructType([
                StructField("inproceedingKey", StringType(), False),
                StructField("author", StringType(), False),
            ])

            typedBy_csv = open("./resources/typed_by.csv", newline="", encoding="utf-8")
            df_data_typedBy = csv.reader(typedBy_csv, delimiter=";")

            df_typedBy = self.spark.createDataFrame(data=df_data_typedBy, schema=self.schemaTypedBy)
            df_typedBy.printSchema()
            df_typedBy.show()
            return df_typedBy
        elif dataFrame == "writtenBy":
            self.schemaWrittenBy = StructType([
                StructField("incollectionKey", StringType(), False),
                StructField("author", StringType(), False),
            ])

            writtenBy_csv = open("./resources/written_by.csv", newline="", encoding="utf-8")
            df_data_writtenBy= csv.reader(writtenBy_csv, delimiter=";")

            df_writtenBy = self.spark.createDataFrame(data=df_data_writtenBy, schema=self.schemaWrittenBy)
            df_writtenBy.printSchema()
            df_writtenBy.show()
            return df_writtenBy
        else:
            print("ERROR: source name wrong...")

    def getSparkSession(self):
        return self.spark

    def getArticleSchema(self):
        return self.schemaArticle

    def getAuthorSchema(self):
        return self.schemaAuthor

    def getJournalSchema(self):
        return self.schemaJournal

    def getComposedBySchema(self):
        return self.schemaComposedBy

    def getBookSchema(self):
        return self.schemaBook

    def getEditedBySchema(self):
        return self.schemaEditedBy

    def getEditorSchema(self):
        return self.schemaEditor

    def getIncollectionSchema(self):
        return self.schemaIncollection

    def getInproceedingSchema(self):
        return self.schemaInproceeding

    def getProceedingSchema(self):
        return self.schemaProceeding

    def getPublisherSchema(self):
        return self.schemaPublisher

    def getRevisedBySchema(self):
        return self.schemaRevisedBy

    def getTypedBySchema(self):
        return self.schemaTypedBy

    def getWrittenBySchema(self):
        return self.schemaWrittenBy