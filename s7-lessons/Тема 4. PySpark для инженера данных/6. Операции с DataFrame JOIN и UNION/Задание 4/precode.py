import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder \
                    .master("local") \
                    .appName("Learning DataFrames") \
                    .getOrCreate()
# данные первого датафрейма 
book_1 = [('Harry Potter and the Goblet of Fire', 'J. K. Rowling', 322),
        ('Nineteen Eighty-Four', 'George Orwell', 382),
        ('Jane Eyre', 'Charlotte Brontë', 159),
        ('Catch-22', 'Joseph Heller',  174),
        ('The Catcher in the Rye', 'J. D. Salinger',  168),
        ('The Wind in the Willows', 'Kenneth Grahame',  259),
        ('The Mayor of Casterbridge', 'Thomas Hardy',  300),
        ('Bad Girls', 'Jacqueline Wilson',  299)
]
# данные второго датафрейма
book_2 = [
        ('Black Beauty',657 ,'Anna Sewell'),
        ('Artemis Fowl',558,'Eoin Colfer'),
        ('The Magic Faraway Tree', 567,'Enid Blyton'),
        ('The Witches', 567,'Roald Dahl'),
        ('Frankenstein',567 ,'Mary Shelley'),
        ('The Little Prince',557 ,'Antoine de Saint-Exupéry'),
        ('The Truth', 576 ,'Terry Pratchett')
]
# названия атрибутов
columns_1= ['title', 'author', 'book_id']
columns_2 = ['title', 'book_id', 'author']
# создаём датафреймы
df_1 = spark.createDataFrame(data=book_1 , schema=columns_1)
df_2  = spark.createDataFrame(data=book_2 , schema=columns_2)
# напишите ваш код ниже

union = df_1.unionByName(df_2)

union.show(40, False)