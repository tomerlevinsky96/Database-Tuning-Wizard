import psycopg2
import decimal

def calculate_total_hit_ratio(conn):
    cursor = conn.cursor()
    query = "SELECT sum(heap_blks_hit) as total_hits, sum(heap_blks_read) as total_reads FROM pg_statio_user_tables"
    cursor.execute(query)
    result = cursor.fetchone()
    if result:
        hits = result[0]
        reads = result[1]
        if hits + reads == 0:
            return 0
        total_hit_ratio = hits / (hits + reads)
        return total_hit_ratio
    else:
        return None

def convert_memory(memory_amount,TotalPercentAdding):
    # Dictionary to convert memory units to bytes
    unit_to_bytes = {
        'B': 1,
        'KB': 1024,
        'MB': 1024 ** 2,
        'GB': 1024 ** 3,
        'TB': 1024 ** 4,
    }

    # Extract numeric value and unit from memory_amount
    numeric_value = float(memory_amount[:-2])
    original_unit = memory_amount[-2:]
    total_percent_adding = float(TotalPercentAdding)
    # Calculate new memory amount with 90% more data
    new_memory_bytes = numeric_value * (1 + total_percent_adding ) * unit_to_bytes[original_unit]

    # Determine the appropriate unit for the converted value
    for unit in unit_to_bytes:
        if new_memory_bytes < unit_to_bytes[unit] * 1024:
            new_memory_amount = new_memory_bytes / unit_to_bytes[unit]
            return new_memory_amount,unit







def caching(Host, User, Port, DBname, Password, file_path):
    db_params = {
        "dbname": DBname,
        "user": User,
        "password": Password,
        "host": Host,
        "port": Port,
    }

    conn = psycopg2.connect(**db_params)
    cursor = conn.cursor()

    # Get a list of all user tables in the database
    cursor.execute("SELECT tablename FROM pg_tables WHERE schemaname = 'public'")
    tables = [row[0] for row in cursor.fetchall()]



    total_hit_ratio = calculate_total_hit_ratio(conn)
    TotalPercentAdding=95-total_hit_ratio
    query=f"""SHOW shared_buffers"""
    cursor.execute(query)
    MemoryAmount=cursor.fetchone()
    new_memory_amount,unit=convert_memory(MemoryAmount[0],TotalPercentAdding)
    with open(file_path, 'a') as f:
        f.write(f"""\nCashing suggestion:\nTo optimize your PostgreSQL database's performance, it is recommended to  adjust the 'shared_buffers' configuration parameter.\nThis setting controls memory allocation for frequently accessed data, leading to faster queries and smoother operation.\nConsidering your cache hit ratio of {total_hit_ratio} , we propose increasing 'shared_buffers' to {new_memory_amount}{unit}.\nThis should cache a significant part of your working set, reducing disk I/O and improving response times.""")





    


