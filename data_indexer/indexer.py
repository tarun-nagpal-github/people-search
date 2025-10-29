import os
import logging
import boto3
import requests
from opensearchpy import OpenSearch, RequestsHttpConnection, AWSV4SignerAuth

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def opensearch_client():
    timeout = int(os.environ.get("OPENSEARCH_CONNECTION_TIMEOUT", 60))
    use_local = os.environ.get("LOCAL", "True").lower() in ("true", "1", "yes")
    
    if use_local:
        os_host = os.environ.get("OPENSEARCH_HOSTS", "localhost")
        os_username = os.environ.get("OPENSEARCH_USERNAME", "")
        os_password = os.environ.get("OPENSEARCH_PASSWORD", "")
        
        # Only use auth if credentials are provided
        auth = None
        if os_username and os_password:
            auth = (os_username, os_password)
            
        return OpenSearch(
            hosts=[{'host': os_host, 'port': 9200}],
            http_auth=auth,
            use_ssl=False,
            verify_certs=False,
            connection_class=RequestsHttpConnection,
            timeout=timeout
        )
    else:
        os_host = os.environ.get("OPENSEARCH_HOSTS", "")
        if not os_host:
            raise ValueError("OPENSEARCH_HOSTS must be set when not using LOCAL mode")
            
        credentials = boto3.Session().get_credentials()
        region = os.environ.get("AWS_REGION", "us-east-1")
        auth = AWSV4SignerAuth(credentials, region)
        return OpenSearch(
            hosts=[{'host': os_host, 'port': 443}],
            http_auth=auth,
            use_ssl=True,
            verify_certs=True,
            ca_certs='/usr/local/share/ca-certificates/McK_Entrust_Root_G1.crt',
            connection_class=RequestsHttpConnection,
            timeout=timeout
        )


def test_opensearch_connection():
    """Test the OpenSearch connection and return True if successful."""
    try:
        logging.info("Attempting to connect to OpenSearch...")
        client = opensearch_client()
        
        # Try to get cluster info to verify connection
        info = client.info()
        logging.info("✓ Successfully connected to OpenSearch!")
        logging.info(f"Cluster name: {info.get('cluster_name', 'N/A')}")
        logging.info(f"OpenSearch version: {info.get('version', {}).get('number', 'N/A')}")
        return True
        
    except Exception as e:
        logging.error(f"✗ Failed to connect to OpenSearch: {str(e)}", exc_info=True)
        return False


def fetch_and_index_users(api_url="https://randomuser.me/api/?results=50", index_name="expertise"):
    """
    Fetch user data from the randomuser.me API and index it into OpenSearch.
    
    Args:
        api_url (str): The API URL to fetch data from. Defaults to randomuser.me API with 50 results.
        index_name (str): The name of the OpenSearch index to store the documents. Defaults to 'people'.
    
    Returns:
        dict: A dictionary with 'success' (bool), 'total_users' (int), and 'indexed_count' (int).
    """
    try:
        logging.info(f"Fetching user data from API: {api_url}")
        response = requests.get(api_url, timeout=30)
        response.raise_for_status()
        
        data = response.json()
        users = data.get('results', [])
        
        if not users:
            logging.warning("No users found in API response")
            return {
                'success': False,
                'total_users': 0,
                'indexed_count': 0,
                'error': 'No users found in API response'
            }
        
        logging.info(f"Received {len(users)} users from API")
        
        # Get OpenSearch client
        client = opensearch_client()
        
        # Check if index exists
        index_exists = client.indices.exists(index=index_name)
        
        if not index_exists:
            logging.info(f"Creating index: {index_name}")
            client.indices.create(
                index=index_name,
                body={
                    "settings": {
                        "number_of_shards": 1,
                        "number_of_replicas": 0
                    }
                }
            )
        else:
            # Index exists, try to ensure it's open
            # We'll try to open it - this will succeed if closed, or do nothing if already open
            try:
                logging.info(f"Ensuring index {index_name} is open...")
                client.indices.open(index=index_name)
                logging.info(f"Index {index_name} is now open")
            except Exception as e:
                error_str = str(e)
                # If it's already open, that's fine - just log it
                if 'index_not_closed_exception' in error_str or 'already open' in error_str.lower():
                    logging.debug(f"Index {index_name} is already open")
                else:
                    # Some other error - try to verify by checking stats
                    try:
                        client.indices.stats(index=index_name)
                        logging.debug(f"Index {index_name} is accessible")
                    except Exception:
                        logging.warning(f"Could not verify index {index_name} state: {str(e)}")
        
        # Index each user as a document
        indexed_count = 0
        errors = []
        
        for idx, user in enumerate(users):
            try:
                # Use the user's UUID as the document ID for deduplication
                doc_id = user.get('login', {}).get('uuid', f"user_{idx}")
                
                # Index the document
                result = client.index(
                    index=index_name,
                    id=doc_id,
                    body=user,
                    refresh=True  # Refresh immediately to make it searchable
                )
                
                if result.get('result') in ['created', 'updated']:
                    indexed_count += 1
                    logging.debug(f"Indexed user {idx + 1}/{len(users)}: {doc_id}")
                    
            except Exception as e:
                error_msg = f"Failed to index user {idx + 1}: {str(e)}"
                logging.error(error_msg)
                errors.append(error_msg)
        
        logging.info(f"Successfully indexed {indexed_count}/{len(users)} users into index '{index_name}'")
        
        result = {
            'success': indexed_count > 0,
            'total_users': len(users),
            'indexed_count': indexed_count,
            'index_name': index_name
        }
        
        if errors:
            result['errors'] = errors
            
        return result
        
    except requests.exceptions.RequestException as e:
        error_msg = f"Failed to fetch data from API: {str(e)}"
        logging.error(error_msg, exc_info=True)
        return {
            'success': False,
            'total_users': 0,
            'indexed_count': 0,
            'error': error_msg
        }
    except Exception as e:
        error_msg = f"Unexpected error: {str(e)}"
        logging.error(error_msg, exc_info=True)
        return {
            'success': False,
            'total_users': 0,
            'indexed_count': 0,
            'error': error_msg
        }


if __name__ == "__main__":
    success = test_opensearch_connection()
    if success:
        print("OpenSearch connection test: PASSED")
        result = fetch_and_index_users()
        if result['success']:
            print("Users indexed successfully")
        else:
            print("Failed to index users")
            exit(1)
       
    else:
        print("OpenSearch connection test: FAILED")
        exit(1)

