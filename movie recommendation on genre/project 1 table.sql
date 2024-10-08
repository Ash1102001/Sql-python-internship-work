-- Databricks notebook source
-- MAGIC %md
-- MAGIC #MOVIE RECOMMENDATION USING GENRES TABLES

-- COMMAND ----------

-- CALLING OUR DATASET FOR STUDYING 
SELECT * FROM movies_3

-- COMMAND ----------

-- SELECTING DESIRED COLUMNS
select title , popularity from movies_3

-- COMMAND ----------

-- CREATING TEMP VIEW TO FILTER THE DATASET
CREATE or replace temp view temp_title as
SELECT title, explode(split(genres, ' ')) AS genres_array
FROM movies_3_csv ORDER BY genres_array;

select * from temp_title

-- COMMAND ----------

-- TEMP VIEW TO FILTER OUT SPECIFIC TITLES WITH CLEANED GENRES 
create or replace temp view title_list as
select title from temp_title where genres_array in (select alias2.genres_array from (
select genres_array, count(*) as c from
temp_title group by genres_array) as alias2 where alias2.c > 3 )

-- COMMAND ----------

-- FINAL CLEANED DATASET FOR MAKING PIVOTED TABLE LATER ON
create or replace table movies_cleaned as 
select * , row_number() over(order by title) as rn1 from 
(select m.index, m.title, m.popularity, m.genres, m.cast, m.director,m.homepage ,m.overview ,row_number() over (partition by m.title order by m.title) as rn from movies_3_csv m join title_list tl on tl.title = m.title) as clean where clean.rn = 1 order by clean.title

-- COMMAND ----------

CREATE or replace temp view exploded_table as
SELECT rn1, explode(split(genres, ' ')) AS genres_array
FROM movies_cleaned ORDER BY genres_array;

select * from exploded_table

-- COMMAND ----------

create or replace temp view genres_list as
select genres_array, count(*) as mov_count from 
(SELECT rn1, explode(split(genres, ' ')) AS genres_array
FROM movies_cleaned ORDER BY genres_array) as a group by a.genres_array order by a.genres_array;

select * from genres_list

-- COMMAND ----------

-- ASSIGNING INTEGER VALUES TO EACH GENRES
create or replace table diamond_table as
select genres_array, mov_count,row_number() over (order by genres_array) as genres_value from genres_list where mov_count > 4;

select * from diamond_table

-- COMMAND ----------

-- RELATING INDEX VALUES OF THEIR CORRESPONDING TITLES TO THEIR RESPECTIVE GENRES
CREATE OR REPLACE TABLE final_table as
SELECT rn1, genres_array
FROM exploded_table where genres_array in (select genres_array from genres_list where mov_count > 4) ORDER BY genres_array;

select * from final_table

-- COMMAND ----------

-- COMBINING TWO TABLES IN ORDER TO ASSIGN INTEGER VALUES TO GENRES WITH RESPECT TO THEIR TITLES
create or replace table plat_table as
select f.rn1,f.genres_array, d.genres_value from final_table f join diamond_table d on f.genres_array = d.genres_array;

select * from plat_table

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # CREATING DATAFRAME OF THE ABOVE TABLE IN ORDER TO PIVOT IT 
-- MAGIC import numpy as np
-- MAGIC df = spark.table("plat_table")
-- MAGIC # display(df)
-- MAGIC df_panda= df.toPandas()
-- MAGIC df_panda['genres_value'] = df_panda['genres_value'].astype(np.int64)
-- MAGIC df_panda['genres_array'] = df_panda['genres_array'].astype('string')
-- MAGIC df_panda['rn1'] = df_panda['rn1'].astype(np.int64)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # PIVOTING THE DATAFRAME 
-- MAGIC pivoted_table = df_panda.pivot(index='rn1', columns= "genres_array", values= "genres_value").fillna(0)
-- MAGIC display(pivoted_table)
