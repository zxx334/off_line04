from pyspark.sql import SparkSession

spark = (SparkSession.builder
         .appName("MySQL Test")
         .config("spark.jars", r"D:\jar\mysql-connector-j-8.0.33.jar")
         .enableHiveSupport()
         .getOrCreate())

# 将MySQL表注册为Spark临时表
df = (spark.read
      .format("jdbc")
      .option("url", "jdbc:mysql://8.141.116.61:3306/realtime_v1")
      .option("driver", "com.mysql.cj.jdbc.Driver")
      .option("user", "root")
      .option("password", "Zxx123,./")
      .option("dbtable", "information_schema.tables")  # 可以在这里加注释
      .load())

df.createOrReplaceTempView("mysql_tables")

# 现在可以用熟悉的spark.sql()方式写SQL了！
spark.sql("SHOW DATABASES").show()
spark.sql("SELECT * FROM mysql_tables WHERE table_schema = 'realtime_v1'").show()

# # 注册具体的业务表（替换your_table_name为实际表名）
# df_table = (spark.read
#             .format("jdbc")
#             .option("url", "jdbc:mysql://8.141.116.61:3306/realtime_v1")
#             .option("driver", "com.mysql.cj.jdbc.Driver")
#             .option("user", "root")
#             .option("password", "Zxx123,./")
#             .option("dbtable", "your_table_name")  # 在这里替换为你的实际表名
#             .load())

# df_table.createOrReplaceTempView("my_table")

# 像写普通Spark SQL一样查询
# spark.sql("SELECT * FROM my_table LIMIT 10").show()
# spark.sql("SELECT COUNT(*) as total FROM my_table").show()

spark.stop()