-- Databricks notebook source
-- MAGIC %md
-- MAGIC #MOVIE RECOMMENDATION USING GENRES PROGRAM

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from fuzzywuzzy import process
-- MAGIC def movie_recommender(movie_name,Data,n):
-- MAGIC     idx= process.extractOne(movie_name,df_pre_pa['title'])[2]
-- MAGIC     print("movie selected: " ,df_pre_pa['title'][idx], "Index: ",idx)
-- MAGIC     print("Movie recommendation......")
-- MAGIC     Distance, indices = model.kneighbors(Data[idx],n_neighbors = n)
-- MAGIC     # print(Distance, indices)
-- MAGIC     rec_movies={"indices": [i for i in indices], "movies":[df_pre_pa['title'][i].where(i!=idx) for i in indices]}
-- MAGIC     for i in indices:
-- MAGIC         print(df_pre_pa['title'][i].where(i!=idx))
-- MAGIC     #     rec_movies.append(df['title'][i].where(i!=idx))
-- MAGIC     dataframe=pd.DataFrame(rec_movies, columns = ['indices', 'movies'])
-- MAGIC     return(dataframe)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.widgets.text(name="input_movie", defaultValue="Avatar", label="Type movie name")
-- MAGIC input_movie = dbutils.widgets.get("input_movie")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # IMPORTING NECESSARY LIBRARIES
-- MAGIC from sklearn.neighbors import NearestNeighbors
-- MAGIC from scipy.sparse import csr_matrix
-- MAGIC import pyspark.pandas as ps
-- MAGIC import pandas as pd
-- MAGIC
-- MAGIC # CREATING DATAFRAME OF OUR CLEANED TABLE TO INSERT IT INTO FUNCTION
-- MAGIC df_pre = spark.table("movies_cleaned")
-- MAGIC df_pre_pa = df_pre.toPandas()
-- MAGIC
-- MAGIC df = spark.table("plat_table")
-- MAGIC
-- MAGIC df_panda= df.toPandas()
-- MAGIC
-- MAGIC
-- MAGIC pivoted_table = df_panda.pivot(index='rn1', columns= "genres_array", values= "genres_value").fillna(0)
-- MAGIC
-- MAGIC
-- MAGIC # CONVERTING THE DATASET INTO SPARSE MATRIX
-- MAGIC mat_table= csr_matrix(pivoted_table.values)
-- MAGIC
-- MAGIC # TRAINING OR MODEL BY GIVING THE SPARSE MATRIX TABLE
-- MAGIC model= NearestNeighbors(metric= "euclidean", algorithm="brute", n_neighbors=20)
-- MAGIC model.fit(mat_table)
-- MAGIC
-- MAGIC
-- MAGIC rec_mov_frame= movie_recommender(input_movie, mat_table, 10)
-- MAGIC
-- MAGIC # CONVERTING OUR OUTPUT INTO SPARK DATAFRAME FROM PANDAS DATAFRAME TO DISPLAY ON TYHE DASHBOARD
-- MAGIC rec_mov_frame['movies']=rec_mov_frame['movies'].astype("string")
-- MAGIC
-- MAGIC rec_spark_frame = ps.DataFrame(rec_mov_frame).to_spark()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(rec_spark_frame)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC rec_spark_frame.createOrReplaceTempView("temp_frame")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC CREATING TABLES AND VISUALIZATION TO DISPLAY IT ON DASHSBOARD
-- MAGIC

-- COMMAND ----------

select title, genres, homepage from movies_cleaned  where rn1 -1 in (select explode(indices) from temp_frame)

-- COMMAND ----------

select title, popularity from movies_cleaned  where rn1 -1 in (select explode(indices) from temp_frame)

-- COMMAND ----------

select title, cast, director from movies_cleaned  where rn1 -1 in (select explode(indices) from temp_frame)

-- COMMAND ----------

select overview from movies_cleaned  where rn1 -1 in (select explode(indices) from temp_frame)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC %pip install fuzzywuzzy
-- MAGIC %pip install python-Levenshtein 
