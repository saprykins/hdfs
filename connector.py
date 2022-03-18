from hdfs.ext.kerberos import KerberosClient

client = KerberosClient('http://hdfs-nn-1.au.adaltas.cloud:50070')

# works
'''
for d in client.list('/education/'):
# for d in client.list('/'):
    print(d)
'''

# 1 - mkdir
# 2 - mkdir
# 3 - py // file in hdfs

local_path = '/home/erpgray/Downloads/pdfs_202203181001.csv'
# hdfs_path = '/education/cs_2022_spring_1/s.saprykin-cs/lab6/'
hdfs_path = '/education/cs_2022_spring_1/s.saprykin-cs/lab7/articles/'
# hdfs_path = '/education/cs_2022_spring_1/s.saprykin-cs/lab5/pdfs_202203181001.csv'
# client.upload(hdfs_path, local_path, n_threads=1, temp_dir=None, chunk_size=65536, progress=None, cleanup=True, **kwargs)
client.upload(hdfs_path, local_path)


# hdfs dfs -cat /education/cs_2022_spring_1/$USER/lab6/ 
# works



# works
# hdfs dfs -cat /education/cs_2022_spring_1/$USER/lab2/pdfs_202203181001.csv

# works too
# hdfs dfs -cat /education/cs_2022_spring_1/$USER/lab5/

# doesnt works 
# hdfs dfs -cat /education/cs_2022_spring_1/$USER/lab5/pdfs_202203181001.csv

'''
# SET hivevar:username=s.saprykin-cs;
SET hivevar:username=saprykin;

# CREATE EXTERNAL TABLE log4jLogs (
# CREATE EXTERNAL TABLE IF NOT EXISTS cs_2022_spring_1.${username}_articles_ext_7 (
# LOCATION '/education/cs_2022_spring_1/resources/lab7/articles/'

CREATE EXTERNAL TABLE cs_2022_spring_1.${username}_articles_ext_8 (
  id INT,
  author STRING,
  creation_date STRING,
  modification_date STRING,
  creator STRING,
  status STRING,
  text STRING,
  file_id STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
STORED AS TEXTFILE
LOCATION '/education/cs_2022_spring_1/s.saprykin-cs/lab7/articles'
TBLPROPERTIES ('skip.header.line.count'='1');
'''
# LOCATION '/education/cs_2022_spring_1/resources/lab5/pdfs_202203181001.csv'

# select * from cs_2022_spring_1.${username}_articles_ext_8;
# select saprykin_articles_ext_8.id, saprykin_articles_ext_8.author from cs_2022_spring_1.${username}_articles_ext_8;
"""
SET hivevar:username=saprykin;

CREATE TABLE IF NOT EXISTS cs_2022_spring_1.${username}_articles_3 (
  id INT,
  author STRING,
  creation_date STRING,
  modification_date STRING,
  creator STRING,
  status STRING,
  text STRING,
  file_id STRING
)
STORED AS ORC;
"""
# empty
# select * from cs_2022_spring_1.saprykin_articles_3; 


'''
INSERT OVERWRITE TABLE cs_2022_spring_1.${username}_articles_3
SELECT
  id,
  author,
  creation_date,
  modification_date,
  creator,
  status,
  text,
  file_id 
FROM cs_2022_spring_1.${username}_articles_ext_8;
'''

'''
SELECT count(*) AS nb
FROM cs_2022_spring_1.${username}_articles_3;


SELECT *
FROM cs_2022_spring_1.${username}_articles_3
limit 10;
'''


# delete table
# change contexte
'''
use cs_2022_spring_1; 
drop table saprykin_articles_2;
'''