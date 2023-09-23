import psycopg2
import InsertAuotoQuerriesToDB
import os


def count_rows_with_non_null_values(table_name, column_names,conn):
    # Construct the WHERE clause of the SQL query using the column names
    where_clause = " AND ".join(f"{col} IS NOT NULL" for col in column_names)

    # Construct the full SQL query using the table name and WHERE clause
    query = f"SELECT COUNT(*) FROM {table_name} WHERE {where_clause}"

    # Execute the query and return the result
    conn.execute(query)
    count = conn.fetchone()[0]
    return count

def extract_index_name(index_query):
    # Extract the index name from the CREATE INDEX query
    index_start = index_query.find('INDEX') + len('INDEX') + 1

    # Find the position of the first space after the index name
    index_end = index_query.find(' ', index_start)

    # Extract the index name using the found positions
    index_name = index_query[index_start:index_end]
    return index_name

def execute_query(conn,engine,query,index_queries):
        conn.execute("SET enable_seqscan = off;")
        engine.commit()
        conn.execute('EXPLAIN ANALYZE ' + query[0][0])
        result = conn.fetchall()
        # Parse the execution plan to extract the index information
        for row in result:
            if "Index" in row[0]:
                index_used = row[0]
                break
        else:
            index_used = "No index information found"
        # Identify the index used from the index_queries array
        used_index = None
        for index_query in index_queries:
            if extract_index_name(index_query) in index_used:
                used_index = index_query
                break
        for index_query in index_queries:
                query = f"DROP INDEX {extract_index_name(index_query)}"
                conn.execute(query)
                engine.commit()
        return used_index


def GenereateIndex(Host,User,Port,DBname,Password,file_path,conn2,cur):
 # Configure the database connection
 engine= psycopg2.connect(
 # Replace the following variables with your database information
  host = Host,
  port = Port,
  database = DBname,
  user = User,
  password = Password,
 )
 conn = engine.cursor()
 conn.execute("""
 SELECT distinct table_name
 FROM information_schema.columns 
 WHERE table_schema = 'public' and table_name<> 'pg_stat_statements_info' and table_name<> 'schema_columns'and table_name<>'pg_stat_statements' and table_name<> 'schema_tables_names' 
 and table_name<>'field' and table_name<>'indexfields' and table_name<>'fieldpositioninindex' and table_name<>'queries' and table_name<>'pg_stat_statment3'
 and table_name<>'madeupqueries'
 """)
 table_names = conn.fetchall()
 countrows=0
 SmallTables=[]
 for table_name in table_names:
   table_name = table_name[0]
   table_name= table_name.replace("('", "").replace("',)", "")
   conn.execute(f" SELECT distinct COUNT(*) FROM {table_name}")
   count_tuple = conn.fetchone()
   count = count_tuple[0]
   if count<=1000:
       SmallTables.append(table_name)
   countrows += int(count)
 # fetch all the resultsB
 global output
 IndexCreatedArray = []
 QueryExecutionTime=[]
 cur.execute("SELECT QueryText FROM queries")
 QueryText=cur.fetchall()
 cur.execute("SELECT queryid FROM queries")
 QueryId = cur.fetchall()
 query = "select f.tablename,f.fieldname  from FieldPositionInIndex  as FPI  inner join  Queries as q on FPI.indexid=q.indexid inner join field as f on f.id=FPI.fieldid where queryid= ? "
 values = QueryId[0]  # Wrap j in a tuple
 cur.execute(query, values)
 FieldsJoin = cur.fetchall()
 from itertools import combinations
     # group the columns by their corresponding table names
 table_columns = {}
 for table, column in FieldsJoin:
       if table in table_columns:
           table_columns[table].append(column)
       else:
           table_columns[table] = [column]
  # generate unique permutations of columns within each table
 for table, columns in table_columns.items():
      generated_combinations = set()
      for r in range(1, len(columns) + 1):
         for comb in combinations(columns, r):
            IndexCreated = f"CREATE INDEX idx_{table}_{'_'.join(comb)}_ ON {table} ({', '.join(comb)});"
            column_names = ', '.join(comb)
            column_names = column_names.split(',')
            countrowsTemp = count_rows_with_non_null_values(table, column_names, conn)
            if table  not in SmallTables:
                if countrowsTemp <= 0.05* countrows :
                    if IndexCreated not in IndexCreatedArray:
                       generated_combinations.add(comb)
                       IndexCreatedArray.append(IndexCreated)
 for index_query in IndexCreatedArray:
     conn.execute(index_query)
 engine.commit()
 index_used = execute_query(conn,engine,QueryText,IndexCreatedArray)
 with open(file_path, 'a') as f:
     f.write('\nIndex suggestion:\n')
     f.write(index_used+ '\n')





