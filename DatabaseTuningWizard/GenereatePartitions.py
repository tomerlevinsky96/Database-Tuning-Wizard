import psycopg2
import pandas as pd
import os
import InsertAuotoQuerriesToDB
import sqlite3
import time
# Replace the following variables with your database information
host = 'localhost'
port = 5433
database = 'appdb1'
user = 'tomer'
password = 't1'


def create_range_partitions(table_name1,column_name1,conn,file_path):
    cursor = conn.cursor()
    CountPartitiones=f"""SELECT COUNT(DISTINCT to_char({column_name1}, 'YYYY_MM')) FROM {table_name1};"""
    cursor.execute(CountPartitiones)
    count = cursor.fetchone()
    if count[0]<=1024:
        distinct_values_query = f"SELECT EXTRACT(YEAR FROM {column_name1}) * 100 + EXTRACT(MONTH FROM {column_name1}), COUNT(*) FROM {table_name1} GROUP BY 1 ORDER BY 1;;"
        cursor.execute(distinct_values_query)
        partition_row_counts = cursor.fetchall()
        for partition_id, rows_count in partition_row_counts:
            if rows_count>3000000:
                return 0
        create_partition = f"""\n CREATE TABLE IF NOT EXISTS {table_name1}_partition (LIKE {table_name1}) PARTITION BY RANGE ({column_name1});
                           CREATE OR REPLACE FUNCTION create_range_partitions()
                           RETURNS VOID AS $$
                           DECLARE
                               start_date DATE;
                               end_date DATE;
                               partition_name TEXT;
                           BEGIN
                             FOR partition_name IN (
                                 SELECT DISTINCT to_char({column_name1}, 'YYYY_MM')
                                 FROM {table_name1}
                             ) LOOP
                              start_date := to_date(partition_name || '-01', 'YYYY_MM_DD');
                              end_date := (start_date + INTERVAL '1 MONTH')::DATE;

                              EXECUTE format('CREATE TABLE partition_%s PARTITION OF {table_name1}_partition FOR VALUES FROM (%L) TO (%L)',
                              partition_name,
                              start_date,
                              end_date
                             );
                            END LOOP;
                            END;
                            $$ LANGUAGE plpgsql;
                            SELECT create_range_partitions();
                            insert into {table_name1}_partition select * from {table_name1}
                         """
        with open(file_path, 'a') as f:
          f.write(create_partition)


def create_list_partitions(table_name1, column_name1,conn,file_path):
    cursor = conn.cursor()
    countdistinctvalues=f"select count(distinct {column_name1}) from {table_name1}"
    cursor.execute(countdistinctvalues)
    count=cursor.fetchone()
    if count[0] <= 1024:
        distinct_values_query = f"SELECT DISTINCT {column_name1} FROM {table_name1};"
        cursor.execute(distinct_values_query)
        distinct_values = cursor.fetchall()
        for value in distinct_values:
                value_str = value[0] if isinstance(value[0], str) else str(value[0])
                count_rows_query = f"SELECT COUNT(*) FROM {table_name1} WHERE {column_name1} = '{value_str}';"
                cursor.execute(count_rows_query)
                rows_count = cursor.fetchone()[0]
                if rows_count > 3000000:
                    return 0
        create_partition=f"""\n CREATE TABLE IF NOT EXISTS {table_name1}_partition (LIKE {table_name1}) PARTITION BY LIST({column_name1});
                          CREATE OR REPLACE FUNCTION create_list_partitions()
                          RETURNS VOID AS $$
                          DECLARE
                             partition_value TEXT;
                             partition_name TEXT;
                          BEGIN
                            FOR partition_value IN (
                                SELECT DISTINCT {column_name1}
                                FROM {table_name1}
                            ) LOOP
                               partition_name := 'partition_' || partition_value;
                               EXECUTE format('CREATE TABLE IF NOT EXISTS %I PARTITION OF {table_name1}_partition FOR VALUES IN (%L)',
                                  partition_name,
                                  ARRAY[partition_value]
                               );
                            END LOOP;
                          END;
                          $$ LANGUAGE plpgsql;
                          SELECT create_list_partitions();
                          insert into {table_name1}_partition select * from {table_name1}                         
                     """
        with open(file_path, 'a') as f:
          f.write(create_partition)


def create_hash_partitions(table_name,column_name,conn,file_path):

    cursor = conn.cursor()
    query = f"WITH partition_counts AS (SELECT COUNT(*) AS total_rows,FLOOR(COUNT(*) / 1900000.0) AS required_partitions FROM {table_name}) SELECT required_partitions FROM partition_counts;"
    cursor.execute(query)
    CountPartitions = cursor.fetchone()
    if CountPartitions[0]<=1024:
      cursor.execute(
            f"SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '{table_name}'")
      table_structure = cursor.fetchall()
      create_partition =f"\n CREATE TABLE IF NOT EXISTS {table_name}_partition ("
      column_nameTemp=column_name
      for column_name, data_type in table_structure:
          create_partition += f"{column_name} {data_type}, "
      create_partition = create_partition.rstrip(", ") + ")"
      create_partition += f"PARTITION BY HASH({column_nameTemp});"
      CountPartitions = int(CountPartitions[0])
      for i in range(0, CountPartitions):
         create_partition += '\n' + f"CREATE TABLE pgbench_t_p{str(i)} PARTITION OF {table_name}_partition FOR VALUES WITH (MODULUS {CountPartitions}, REMAINDER {i});"
         rows_query = f"""SELECT PARTITION_NUMBER, COUNT(*) AS ROW_COUNT FROM ( SELECT *, MOD({column_nameTemp}, 16) AS PARTITION_NUMBER FROM {table_name}) AS subquery_alias WHERE PARTITION_NUMBER = {i} GROUP BY PARTITION_NUMBER;"""
         cursor.execute(rows_query)
         partition_row_counts = cursor.fetchall()
         if partition_row_counts[0][1]>3000000:
             return 0
      create_partition+='\n'+ f"""insert into {table_name}_partition
                            select *
                            from {table_name} """
      with open(file_path, 'a') as f:
         f.write(create_partition)


def check_updates(cur, table_name, column_name):


    query = f"""
      SELECT COUNT(*) AS updates_count
      FROM pg_stat_activity
      WHERE query ~* '^UPDATE .*{table_name}.*SET.*{column_name}.*$';
    """

    cur.execute(query)
    results = cur.fetchall()[0]


    return results



def CheckForUpdateAndEmptyColumns(FieldsAndType,table_name,cur,cursor,conn):
  CountEmptyValues = 1
  CountUpdates = 1
  QueryCount = []
  while CountEmptyValues != 0 and CountUpdates != 0:
    for FieldAndType in FieldsAndType:
        query = f"select SUM(q.Calls) from FieldPositionInIndex  as FPI  inner join  Queries as q on FPI.indexid=q.indexid inner join field as f on f.id=FPI.fieldid where f.TableName='{table_name}' and f.FieldName='{FieldAndType[0]}'"
        cur.execute(query)
        result=cur.fetchone()[0]
        if result is None:
           result = 0
        QueryCount.append(result)
    max_index = max(range(len(QueryCount)), key=QueryCount.__getitem__)
    max_cell = FieldsAndType[max_index]
    query = f"SELECT COUNT(*) FROM {table_name} WHERE {max_cell[0]} IS NULL;"
    cursor.execute(query)
    CountEmptyValues = cursor.fetchone()
    conn.commit()
    if CountEmptyValues[0] != 0:
        del FieldsAndType[max_index]
        QueryCount = []
        continue
    query = f"WITH partition_counts AS (SELECT COUNT(*) AS total_rows,FLOOR(COUNT(*) / 2000000.0) AS required_partitions FROM {table_name}) SELECT required_partitions FROM partition_counts;"
    cursor.execute(query)
    CountPartitions = cursor.fetchone()
    if CountPartitions[0]==0:
        del FieldsAndType[max_index]
        QueryCount = []
        continue
    CountUpdates = check_updates(cursor, table_name, max_cell[0])
    if CountEmptyValues[0] != 0:
        del FieldsAndType[max_index]
        QueryCount = []
        continue
    return max_cell

def GenereatePartitions(Host, User, Port, DBname, Password,file_path,conn2,cur):
 # Establish a connection to the PostgreSQL database
 conn = psycopg2.connect(
    host=Host,
    port=Port,
    database=DBname,
    user=User,
    password=Password
 )

 IsOneQuery=False
 file_path3=""
 original_path = os.getcwd()
 stillcontinue="yes"

 # Create a cursor object to execute SQL queries
 cursor = conn.cursor()


 cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='public' and table_name<>'pg_stat_statements_info' and table_name<>'pg_stat_statements' ")
 tables=cursor.fetchall()

 cur.execute("SELECT queryid FROM queries")
 QueryId = cur.fetchall()

 query = "select QueryText,queryid from Queries "
 cur.execute(query)
 queries=cur.fetchall()


 numeric_types = [
    "integer",
    "bigint",
    "decimal(precision, scale)",
    "numeric(precision, scale)"
 ]

 character_types = [
    "character",
    "character varying",
    "varchar",
    "text",
    "time"
 ]

 datetime_types = [
    "date",
    "timestamp",
    "interval"
 ]
 with open(file_path, 'a') as f:
     f.write('\n' +"Partitioning suggestions:")

 for table in tables:
    table_name = table[0]
    cursor.execute(f"SELECT COUNT(*) FROM {table_name} ")
    row_count = cursor.fetchone()[0]
    queryadd=""
    if row_count>=30000000:
      query=f"SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = 'public' AND table_name = '{table_name}'"
      cursor.execute(query)
      FieldsAndType = cursor.fetchall()
      if not FieldsAndType:
           break
      FieldAndType=CheckForUpdateAndEmptyColumns(FieldsAndType,table_name,cur,cursor,conn)

      for numeric_type in numeric_types:
            if numeric_type==FieldAndType[1]:
               create_hash_partitions(table_name, FieldAndType[0], conn,file_path)

      for character_type in character_types:
            if character_type==FieldAndType[1]:
               create_list_partitions(table_name,FieldAndType[0],conn,file_path)

      for datetime_type in datetime_types:
            if datetime_type==FieldAndType[1]:
                create_range_partitions(table_name,FieldAndType[0],conn,file_path)





