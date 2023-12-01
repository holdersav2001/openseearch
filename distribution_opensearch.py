import psycopg2
import concurrent.futures
from opensearchpy import OpenSearch, helpers

# Database connection parameters
db_params = {
    'dbname': 'ods',
    'user': 'postgres',
    'password': 'postgres',
    'host': 'localhost'
}

# OpenSearch connection parameters
opensearch_params = {
    'hosts': [{'host': 'localhost', 'port': 9200}],
    'http_auth': ('admin', 'admin'),  # If authentication is required
    'use_ssl': True,  # Use True for https and False for http
    'verify_certs': False
}

# Connect to OpenSearch
es_client = OpenSearch(**opensearch_params)

# Function to execute the SQL query and return the result
def fetch_data(executor_id, distribution_range, total_executors):
    try:
        connection = psycopg2.connect(**db_params)
        cursor = connection.cursor()

        # Calculate distribution value
        distribution_value = executor_id % distribution_range

        # SQL query
        query = f"""
        SELECT ABS(('x' || MD5(account_no))::BIT(32)::INT) % {distribution_range} AS distribution, 
               account_no, security_no, sum(quantity) as quantity
        FROM transaction_ods.position 
        WHERE (ABS(('x' || MD5(account_no))::BIT(32)::INT) % {distribution_range})::int = {distribution_value} 
        GROUP BY distribution, account_no, security_no 
        """
        cursor.execute(query,)
        return cursor.fetchall()
    except Exception as e:
        print(f"Error in fetch_data: {str(e)}")
    finally:
        connection.close()

# Function to prepare and insert data into OpenSearch  
def insert_into_opensearch(data_chunk):
    actions = [
        {
            "_index": "position_aggregate",
            "_source": {
                "distribution": row[0],
                "account_no": row[1],
                "security_no": row[2],
                "total_quantity": row[3]
            }
        }
        for row in data_chunk
    ]

    if actions:
        helpers.bulk(es_client, actions)

def main(distribution_range, num_workers):
    # Delete existing data in OpenSearch index
    es_client.indices.delete(index="position_aggregate", ignore=[400, 404])

    with concurrent.futures.ThreadPoolExecutor(max_workers=num_workers) as executor:
        # Fetch and process data in parallel
        futures = [executor.submit(fetch_data, i, distribution_range, num_workers) for i in range(num_workers)]
        data_chunks = [future.result() for future in concurrent.futures.as_completed(futures)]

        # Insert data into OpenSearch in parallel
        insert_futures = [executor.submit(insert_into_opensearch, chunk) for chunk in data_chunks]
        concurrent.futures.wait(insert_futures)

if __name__ == "__main__":
    distribution_range = 4
    num_workers = 4
    main(distribution_range, num_workers)
