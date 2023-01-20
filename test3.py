from pyspark.sql import SparkSession
sparkdriver = SparkSession.builder.master('local').appName('demoapp1').getOrCreate()

if __name__== '__main__':
    df_pyspark=sparkdriver.read.option('header','true').csv('DIM_PLAYER.csv',inferSchema=True)
    print(df_pyspark.take(2))
    df_pyspark.printSchema()
    print(type(df_pyspark))

    df_pyspark.select('Player_Name').show()
    print(df_pyspark.dtypes)
    df_pyspark.describe().show()

    df_pyspark.withColumn()
