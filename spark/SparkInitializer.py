import findspark
findspark.init()
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType
import csv
import datetime


class SparkInitializer:
    def __init__(self, master, appName):
        #current spark session
        self.__spark = SparkSession.builder.master(master).appName(appName).getOrCreate()

        # schema for article dataframe
        self.__articleSchema = StructType([
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
            StructField("journal", StringType(), True)
        ])

        #schema for author dataframe
        self.__authorSchema = StructType([
            StructField("name", StringType(), False),
            StructField("email", StringType(), True),
            StructField("affiliation", StringType(), True)
        ])

        #schema for book dataframe
        self.__bookSchema = StructType([
            StructField("key", StringType(), False),
            StructField("title", StringType(), True),
            StructField("mdate", DateType(), True),
            StructField("year", IntegerType(), True),
            StructField("isbn", StringType(), True),
            StructField("url", StringType(), True),
            StructField("ee", StringType(), True),
            StructField("publisher", StringType(), True)
        ])

        #schema for composedBy dataframe
        self.__composedBySchema = StructType([
            StructField("articleKey", StringType(), False),
            StructField("author", StringType(), False),
        ])

        #schema for editedBy dataframe
        self.__editedBySchema = StructType([
            StructField("bookKey", StringType(), False),
            StructField("editor", StringType(), False),
        ])

        #schema for editor dataframe
        self.__editorSchema = StructType([
            StructField("name", StringType(), False),
            StructField("email", StringType(), True),
            StructField("country", StringType(), True)
        ])

        #schema for incollection dataframe
        self.__incollectionSchema = StructType([
            StructField("key", StringType(), False),
            StructField("title", StringType(), True),
            StructField("mdate", DateType(), True),
            StructField("year", IntegerType(), True),
            StructField("publType", StringType(), True),
            StructField("pages", StringType(), True),
            StructField("url", StringType(), True),
            StructField("ee", StringType(), True),
            StructField("bookKey", StringType(), True)
        ])

        #schema for inproceeding dataframe
        self.__inproceedingSchema = StructType([
            StructField("key", StringType(), False),
            StructField("title", StringType(), True),
            StructField("mdate", DateType(), True),
            StructField("year", IntegerType(), True),
            StructField("pages", StringType(), True),
            StructField("url", StringType(), True),
            StructField("ee", StringType(), True),
            StructField("proceedingKey", StringType(), True)
        ])

        #schema for journal dataframe
        self.__journalSchema = StructType([
            StructField("name", StringType(), False)
        ])

        #schema for proceeding dataframe
        self.__proceedingSchema = StructType([
            StructField("key", StringType(), False),
            StructField("title", StringType(), True),
            StructField("mdate", DateType(), True),
            StructField("year", IntegerType(), True),
            StructField("series", StringType(), True),
            StructField("isbn", StringType(), True),
            StructField("url", StringType(), True),
            StructField("ee", StringType(), True),
            StructField("publisher", StringType(), True)
        ])

        #schema for publisher dataframe
        self.__publisherSchema = StructType([
            StructField("name", StringType(), False),
            StructField("headquarter", StringType(), True)
        ])

        #schema for revisedBy dataframe
        self.__revisedBySchema = StructType([
            StructField("proceedingKey", StringType(), False),
            StructField("editor", StringType(), False),
        ])

        #schema for typedBy dataframe
        self.__typedBySchema = StructType([
            StructField("inproceedingKey", StringType(), False),
            StructField("author", StringType(), False),
        ])

        #schema for writtenBy dataframe
        self.__writtenBySchema = StructType([
            StructField("incollectionKey", StringType(), False),
            StructField("author", StringType(), False),
        ])

        #Initializing the csv source
        self.__fileCSV = None

        #Dictionary that will contain the entire dataset
        self.__dataset = {}


    #Private methods
    #Given a data source name, it returns its data in a csv reader type
    def __readDataSource(self, dataSourceName):
        self.__fileCSV = open("./resources/" + dataSourceName + ".csv", newline="", encoding="utf-8")
        reader = csv.reader(self.__fileCSV, delimiter=";")
        return reader

    #Given an integer in format string it returns the cast integer if it exists, none if the field is null
    def __castInteger(self, toCast):
        try:
            return int(toCast)
        except:
            return None

    #Print schema and the first 20 rows of a given dataframe
    def __printDataframe(self, dataframe):
        dataframe.printSchema()
        dataframe.show()


    #Public methods
    #Given a dataframe name, build the dataframe object and return it
    #If verbose is set to True, it will also print the dataframe schema and its first 20 rows
    def createCustomizedDataFrame(self, dataFrameName, verbose=True):
        df = None

        if dataFrameName == "articles":
            articlesReader = self.__readDataSource(dataFrameName)

            # data parsing...
            df_data_articles = []
            for row in articlesReader:
                row[2] = datetime.datetime.strptime(row[2], '%Y-%m-%d')
                row[3] = self.__castInteger(row[3])
                row[8] = self.__castInteger(row[8])
                row[9] = self.__castInteger(row[9])
                df_data_articles.append(row)

            df = self.__spark.createDataFrame(data=df_data_articles, schema=self.__articleSchema)
        elif dataFrameName=="authors":
            authorsReader = self.__readDataSource(dataFrameName)

            df = self.__spark.createDataFrame(data = authorsReader, schema=self.__authorSchema)
        elif dataFrameName=="books":
            booksReader = self.__readDataSource(dataFrameName)

            #data parsing...
            df_data_books = []
            for row in booksReader:
                row[2] = datetime.datetime.strptime(row[2], '%Y-%m-%d')
                row[3] = self.__castInteger(row[3])
                df_data_books.append(row)

            df = self.__spark.createDataFrame(data=df_data_books, schema=self.__bookSchema)
        elif dataFrameName=="composed_by":
            composedByReader = self.__readDataSource(dataFrameName)

            df = self.__spark.createDataFrame(data=composedByReader, schema=self.__composedBySchema)
        elif dataFrameName=="edited_by":
            editedByReader = self.__readDataSource(dataFrameName)

            df = self.__spark.createDataFrame(data=editedByReader, schema=self.__editedBySchema)
        elif dataFrameName=="editors":
            editorsReader = self.__readDataSource(dataFrameName)

            df = self.__spark.createDataFrame(data=editorsReader, schema=self.__editorSchema)
        elif dataFrameName == "incollections":
            incollectionsReader = self.__readDataSource(dataFrameName)

            #data parsing...
            df_data_incollections = []
            for row in incollectionsReader:
                row[2] = datetime.datetime.strptime(row[2], '%Y-%m-%d')
                row[3] = self.__castInteger(row[3])
                df_data_incollections.append(row)

            df = self.__spark.createDataFrame(data=df_data_incollections, schema=self.__incollectionSchema)
        elif dataFrameName == "inproceedings":
            inproceedingsReader = self.__readDataSource(dataFrameName)

            df_data_inproceedings = []
            for row in inproceedingsReader:
                row[2] = datetime.datetime.strptime(row[2], '%Y-%m-%d')
                row[3] = int(row[3])
                df_data_inproceedings.append(row)

            df = self.__spark.createDataFrame(data=df_data_inproceedings, schema=self.__inproceedingSchema)
        elif dataFrameName == "journals":
            journalsReader = self.__readDataSource(dataFrameName)

            #data parsing...
            df_data_books = []
            for row in journalsReader:
                record = []
                record.append(row[0])
                df_data_books.append(record)

            df = self.__spark.createDataFrame(data=df_data_books, schema=self.__journalSchema)
        elif dataFrameName == "proceedings":
            proceedingsReader = self.__readDataSource(dataFrameName)

            df_data_proceedings = []
            for row in proceedingsReader:
                row[2] = datetime.datetime.strptime(row[2], '%Y-%m-%d')
                row[3] = self.__castInteger(row[3])
                df_data_proceedings.append(row)

            df = self.__spark.createDataFrame(data=df_data_proceedings, schema=self.__proceedingSchema)
        elif dataFrameName == "publishers":
            publishersReader = self.__readDataSource(dataFrameName)

            df= self.__spark.createDataFrame(data=publishersReader, schema=self.__publisherSchema)
        elif dataFrameName == "revised_by":
            revisedByReader = self.__readDataSource(dataFrameName)

            df = self.__spark.createDataFrame(data=revisedByReader, schema=self.__revisedBySchema)
        elif dataFrameName == "typed_by":
            typedByReader = self.__readDataSource(dataFrameName)

            df = self.__spark.createDataFrame(data=typedByReader, schema=self.__typedBySchema)
        elif dataFrameName == "written_by":
            writtenByReader = self.__readDataSource(dataFrameName)

            df = self.__spark.createDataFrame(data=writtenByReader, schema=self.__writtenBySchema)
        else:
            print("ERROR: source name wrong...")
            return

        #closure of csv source
        self.__fileCSV.close()

        if verbose:
            df.printSchema()
            df.show()
        self.__dataset[dataFrameName] = df
        return df


    #getter for current spark session
    def getSparkSession(self):
        return self.__spark

    #getter for article schema
    def getArticleSchema(self):
        return self.__articleSchema

    #getter for author schema
    def getAuthorSchema(self):
        return self.__authorSchema

    #getter for journal schema
    def getJournalSchema(self):
        return self.__journalSchema

    #getter for composedBy schema
    def getComposedBySchema(self):
        return self.__composedBySchema

    #getter for book schema
    def getBookSchema(self):
        return self.__bookSchema

    #getter for editedBy schema
    def getEditedBySchema(self):
        return self.__editedBySchema

    #getter for editor schema
    def getEditorSchema(self):
        return self.__editorSchema

    #getter for incollection schema
    def getIncollectionSchema(self):
        return self.__incollectionSchema

    #getter for inproceeding schema
    def getInproceedingSchema(self):
        return self.__incollectionSchema

    #getter for proceeding schema
    def getProceedingSchema(self):
        return self.__proceedingSchema

    #getter for publisher schema
    def getPublisherSchema(self):
        return self.__publisherSchema

    #getter for revisedBy schema
    def getRevisedBySchema(self):
        return self.__revisedBySchema

    #getter for typedBy schema
    def getTypedBySchema(self):
        return self.__typedBySchema

    #getter for writtenBy schema
    def getWrittenBySchema(self):
        return self.__writtenBySchema

    #getter for the entire built dataset
    def getWholeDataset(self):
        return self.__dataset


