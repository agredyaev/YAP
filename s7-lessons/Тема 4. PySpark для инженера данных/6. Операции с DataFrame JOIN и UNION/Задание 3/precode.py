import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder \
                    .master("local") \
                    .appName("Learning DataFrames") \
                    .getOrCreate()
# данные первого датафрейма 
book = [('Harry Potter and the Goblet of Fire', 'J. K. Rowling', 322),
        ('Nineteen Eighty-Four', 'George Orwell', 382),
        ('Jane Eyre', 'Charlotte Brontë', 159),
        ('Catch-22', 'Joseph Heller',  174),
        ('The Catcher in the Rye', 'J. D. Salinger',  168),
        ('The Wind in the Willows', 'Kenneth Grahame',  259),
        ('The Mayor of Casterbridge', 'Thomas Hardy',  300),
        ('Bad Girls', 'Jacqueline Wilson',  299)
]
# данные второго датафрейма
library = [
        ( 322, "1"),
        ( 250, "2" ),
        (400, "2"),
        (159, "1"),
        (382, "2"),
        (322, "1")
]
# названия атрибутов
columns = ['title', 'author', 'book_id']
columns_library = ['book_id', 'Library_id']
# создаём датафреймы
df = spark.createDataFrame(data=book, schema=columns)
df_library  = spark.createDataFrame(data=library, schema=columns_library )
# напишите ваш код ниже

joinded2 = df.join(df_library, ['book_id'], how='inner')

joinded2.select('title').show(40, False)