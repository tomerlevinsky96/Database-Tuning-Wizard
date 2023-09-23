import psycopg2
import sqlite3
import pandas as pd
import re
import os
# Define the fieldTable array as a list of tuples

# Connect to the SQLite database


def generate_the_get_query(table_names):
    table_conditions = " OR ".join([f"s.query ILIKE '%{table_name}%'" for table_name in table_names])
    query = f"""
       SELECT subquery.query,
       subquery.avg_exec_time,
       subquery.calls
       FROM (
         SELECT s.query,
                s.total_exec_time / NULLIF(s.calls, 0) AS avg_exec_time,
                s.calls
        FROM pg_stat_statements AS s
        JOIN pg_stat_activity AS a ON s.userid = a.usesysid
        WHERE ({table_conditions})
            AND (s.query ILIKE 'SELECT%' OR s.query ILIKE 'UPDATE%' OR s.query ILIKE 'DELETE%')
            AND a.datname = current_database()
            AND s.query NOT ILIKE 'pg_%'
            AND s.query NOT ILIKE '%pg_stat_statements%'
            AND s.query NOT ILIKE '%pg_stat_statements_info%'
            AND s.query ILIKE '%WHERE%'
       ) AS subquery
    """
    return query





def InsertAuotoQuerriesToDB(Host,Port,DBname,User,Password,original_path):
 engine= psycopg2.connect(
   # Replace the following variables with your database information
   host = Host,
   port =Port,
   database = DBname,
   user = User,
   password = Password,
  )
 conn = engine.cursor()
 os.chdir(original_path)
 conn2= sqlite3.connect('mydatabase.db')
 cur=conn2.cursor()
 create_table_field = """
        SELECT table_name as TableName, column_name as FieldName
        FROM information_schema.columns
         WHERE table_schema = 'public' 
           AND table_name NOT IN ('pg_stat_statements_info', 'schema_columns', 'pg_stat_statements', 'schema_tables_names', 'FieldPositionInIndex', 'IndexFields', 'Queries', 'madeupqueries', 'indexfields')
          AND column_name NOT IN (
          SELECT kcu.column_name
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
        ON tc.constraint_name = kcu.constraint_name
       WHERE tc.table_schema = 'public'
        AND tc.table_name = information_schema.columns.table_name
       AND tc.constraint_type = 'PRIMARY KEY'
   );
     """
 conn.execute(create_table_field)
 fieldTable=conn.fetchall()

 cur.execute('''
    CREATE TABLE Field (
     TableName TEXT,
     FieldName TEXT,
     id INTEGER PRIMARY KEY AUTOINCREMENT
   )
   ''')

 # Execute the INSERT INTO statement with the "fieldTable" parameter
 for name_table, name_field in fieldTable:
     cur.execute('''
     INSERT INTO Field(TableName,FieldName)
     VALUES (?, ?)
     ''', (name_table, name_field))

 create_table_index_fields = """
        CREATE TABLE IndexFields (
            IndexId SERIAL PRIMARY KEY
        )
      """

 create_table_field_position_in_index = """
      CREATE TABLE FieldPositionInIndex (
           Id SERIAL PRIMARY KEY,
           FieldId INTEGER NOT NULL,
           IndexId INTEGER NOT NULL,
           FieldPosition INTEGER NOT NULL,
           FOREIGN KEY (FieldId) REFERENCES Field(Id) ON DELETE CASCADE,
           FOREIGN KEY (IndexId) REFERENCES IndexFields(IndexId) ON DELETE CASCADE
       )
     """
 create_table_query = """
       CREATE TABLE Queries (
           QueryId INTEGER PRIMARY KEY AUTOINCREMENT,
           QueryText TEXT,
           IndexId INTEGER NOT NULL,
           AvgExecutionTime REAL,
           Calls INTEGER ,
           FOREIGN KEY (IndexId) REFERENCES IndexFields(IndexId) ON DELETE CASCADE
       )
    """
 cur.execute(create_table_index_fields)
 cur.execute(create_table_field_position_in_index)
 cur.execute(create_table_query)
   # Commit the changes and close the connection
 conn2.commit()
 FieldsTables = pd.read_sql_query("select distinct FieldName FROM Field", conn2)
 TablesNames = pd.read_sql_query("select distinct TableName FROM Field", conn2)
 QuerryData = []
 query = f"""SELECT table_name 
                 FROM information_schema.tables
                 WHERE table_schema = 'public' and table_name not in ('pg_stat_statements_info','pg_stat_statements') ; """
 conn.execute(query)
 TablesArray=conn.fetchall()
 TablesArray = [item[0] for item in TablesArray]
 query=generate_the_get_query(TablesArray)
 conn.execute(query)
 queries=conn.fetchall()
 Fields = []
 TableNames = []
 for i in range(len(FieldsTables)):
    Fields.append(FieldsTables.iloc[i][0])
 for i in range(len(TablesNames)):
    TableNames.append(TablesNames.iloc[i][0])
 IndexNum = 0
 for i in range(len(queries)):
    StartingIndex = queries[i][0].lower().find("where")
    StartingIndex2 = queries[i][0].lower().find("select")
    if StartingIndex2 == -1:
        StartingIndex2 = queries[i][0].lower().find("update")
        if StartingIndex2 == -1:
            StartingIndex2 =queries[i][0].lower().find("insert")
            if StartingIndex2 == -1:
                StartingIndex2 =queries[i][0].lower().find("delete")
    WhereClause = queries[i][0][StartingIndex:]
    InitClause = queries[i][0][StartingIndex2:StartingIndex]
    CurrentTables = []
    CurrentFields = []
    columnstemp=[]
    for TableName in TableNames:
            if TableName in InitClause:
                CurrentTables.append(TableName)
    for columntemp in Fields:
            if columntemp in WhereClause:
                columnstemp.append(columntemp)
    if len(CurrentTables) != 0 and len(columnstemp)!=0:  # check if tables exsits in InitClause
            query = "insert into IndexFields(IndexId) values(?)"
            values = IndexNum
            cur.execute(query, (values,))
            conn2.commit()
            query = "insert into Queries(QueryText,IndexId,AvgExecutionTime,Calls) values(?,?,?,?)"  # insert query to database
            values = (queries[i][0], IndexNum,queries[i][1],queries[i][2])
            cur.execute(query, values)
            conn2.commit()
            WhereClauseWords = re.findall(r'\b\w+\b', WhereClause)
            for Field in Fields:
                if Field in WhereClauseWords:
                    if Field not in CurrentFields:
                        CurrentFields.append(Field)
            pattern = r'(\w+)\s+AS\s+(\w+)'
            CurrentTables2 = re.findall(pattern, InitClause)
            if len(CurrentTables2) == 0:
                pattern = r'(\w+)\s+as\s+(\w+)'
                CurrentTables2 = re.findall(pattern, InitClause)
            if len(CurrentTables2) != 0:  # check if there is more than 1 table
                pattern = r'(\w+)\s*\.\s*(\w+)'
                CurrentFields2 = re.findall(pattern, WhereClause)
                position = 0
                flag = 0
                for table in CurrentTables2:
                    for Field in CurrentFields2:
                        if Field[0] == table[1]:
                            query = "SELECT EXISTS(SELECT 1  FROM Field WHERE TableName = ? and FieldName =?)"
                            values = (table[0], Field[1])
                            cur.execute(query, values)
                            result = cur.fetchall()
                            if result[0] == True:
                                query = "SELECT id  FROM Field WHERE TableName = ? and FieldName = ? "
                                values = (table[0], Field[1])
                                conn.execute(query, values)
                                id = cur.fetchone()
                                query = "insert into FieldPositionInIndex(FieldId,IndexId,FieldPosition) values(?,?,?)"
                                values = (id[0], IndexNum, position)
                                cur.execute(query, values)
                                conn2.commit()
                                position = position + 1
                IndexNum = IndexNum + 1
            else:  # if there is only  one table
                position = 0
                for CurrentTable in CurrentTables:
                    for CurrentField in CurrentFields:
                        query = "SELECT EXISTS(SELECT 1  FROM Field WHERE TableName = ? and FieldName = ?)"
                        values = (CurrentTable, CurrentField)
                        cur.execute(query, values)
                        result = cur.fetchone()
                        if result[0] == True:
                            query = "SELECT id  FROM Field WHERE TableName = ? and FieldName = ?"
                            values = (CurrentTable, CurrentField)
                            cur.execute(query, values)
                            id = cur.fetchone()
                            query = "insert into FieldPositionInIndex(FieldId,IndexId,FieldPosition) values(?,?,?)"
                            values = (id[0], IndexNum, position)
                            cur.execute(query, values)
                            conn2.commit()
                            position = position + 1
                IndexNum = IndexNum + 1
 return conn2,cur