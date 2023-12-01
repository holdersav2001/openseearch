import psycopg2

# Database connection parameters
db_params = {
    'dbname': 'ods',
    'user': 'postgres',
    'password': 'postgres',
    'host': 'localhost'
}

# Function to execute the SQL query for a specific distribution range and insert the result
def fetch_and_insert_data(distribution_range, executor_id, total_executors): 
    try: 
        connection = psycopg2.connect(**db_params) 
        cursor = connection.cursor() 
        distribution_value = executor_id % distribution_range 

        query = f""" 
        SELECT ABS(('x' || MD5(account_no))::BIT(32)::INT) % {distribution_range} AS distribution, 
               account_no, security_no, sum(quantity) as quantity
        FROM transaction_ods.position 
        WHERE (ABS(('x' || MD5(account_no))::BIT(32)::INT) % {distribution_range})::int = {distribution_value} 
        GROUP BY distribution, account_no, security_no 
        """ 
        print(f"Executor {executor_id}: Query to be executed: {query}")
        print(f"Executor {executor_id}: Distribution Value: {distribution_value}")

        #cursor.execute(query, (distribution_value,)) 
        cursor.execute(query) 

        data = cursor.fetchall() 

        if data: 
            print(f"Executor {executor_id}: Data fetched: {data}")
            insert_query = "INSERT INTO transaction_ods.position_aggregate (distribution, account_no, security_no, quantity) VALUES (%s, %s, %s, %s)" 
            cursor.executemany(insert_query, data) 
            connection.commit() 
        else:
            print(f"Executor {executor_id}: No data fetched")
            print(f"Executor {executor_id}: Data fetched: {data}")

        cursor.close() 
    except Exception as e: 
        print(f"Error in fetch_and_insert_data for executor {executor_id}: {str(e)}") 
        print(data)
    finally: 
        if connection:
            connection.close()

def main(distribution_range, num_workers):
    for executor_id in range(num_workers):
        fetch_and_insert_data(distribution_range, executor_id, num_workers)

if __name__ == "__main__":
    distribution_range = 4  # Example input variable
    num_workers = 4  # Adjust based on your needs and system capabilities
    main(distribution_range, num_workers)
