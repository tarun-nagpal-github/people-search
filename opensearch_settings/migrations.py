import json
import os
import traceback
import logging
import copy

import boto3
from opensearchpy import OpenSearch, RequestsHttpConnection, AWSV4SignerAuth

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class Migrations:

    def __init__(self):
        # Get the base directory of this script
        self.base_dir = os.path.dirname(os.path.abspath(__file__))

    def opensearch_client(self):
        logger.info("=" * 60)
        logger.info("Initializing OpenSearch client connection")
        logger.info("=" * 60)
        
        # Log timeout configuration
        timeout = int(os.environ.get("OPENSEARCH_CONNECTION_TIMEOUT", 60))
        logger.info(f"Connection timeout: {timeout} seconds")
        
        # Log mode determination
        local_env = os.environ.get("LOCAL", "True")
        use_local = local_env.lower() in ("true", "1", "yes")
        logger.info(f"LOCAL environment variable: '{local_env}' -> Using {'LOCAL' if use_local else 'REMOTE'} mode")
        
        if use_local:
            logger.info("--- Configuring LOCAL connection ---")
            os_host = os.environ.get("OPENSEARCH_HOSTS", "localhost")
            os_username = os.environ.get("OPENSEARCH_USERNAME", "")
            os_password = os.environ.get("OPENSEARCH_PASSWORD", "")
            
            logger.info(f"OpenSearch host: {os_host}")
            logger.info(f"Port: 9200")
            logger.info(f"Username provided: {'Yes' if os_username else 'No'}")
            logger.info(f"Password provided: {'Yes' if os_password else 'No'}")
            logger.info(f"SSL enabled: False")
            logger.info(f"Certificate verification: False")
            
            # Only use auth if credentials are provided
            auth = None
            if os_username and os_password:
                auth = (os_username, os_password)
                logger.info("Authentication enabled with username/password")
            else:
                logger.info("No authentication configured (no username/password provided)")
                
            try:
                logger.info(f"Attempting to create OpenSearch client...")
                client = OpenSearch(
                    hosts=[{'host': os_host, 'port': 9200}],
                    http_auth=auth,
                    use_ssl=False,
                    verify_certs=False,
                    connection_class=RequestsHttpConnection,
                    timeout=timeout
                )
                logger.info(f"OpenSearch client created successfully for {os_host}:9200")
                
                # Test connection with a simple info call
                logger.info("Testing connection with cluster info request...")
                info = client.info()
                logger.info(f"Connection test successful! Cluster name: {info.get('cluster_name', 'unknown')}, "
                          f"Version: {info.get('version', {}).get('number', 'unknown')}")
                logger.info("=" * 60)
                return client
            except Exception as e:
                logger.error(f"Failed to create or connect to OpenSearch client: {str(e)}")
                logger.error(f"Exception type: {type(e).__name__}")
                logger.error(f"Traceback:\n{traceback.format_exc()}")
                logger.info("=" * 60)
                raise
        else:
            logger.info("--- Configuring REMOTE connection ---")
            os_host = os.environ.get("OPENSEARCH_HOSTS", "")
            if not os_host:
                logger.error("OPENSEARCH_HOSTS environment variable is not set!")
                raise ValueError("OPENSEARCH_HOSTS must be set when not using LOCAL mode")
                
            logger.info(f"OpenSearch host: {os_host}")
            logger.info(f"Port: 443")
            logger.info(f"SSL enabled: True")
            logger.info(f"Certificate verification: True")
            logger.info(f"CA certificate path: /usr/local/share/ca-certificates/McK_Entrust_Root_G1.crt")
            
            try:
                logger.info("Retrieving AWS credentials...")
                session = boto3.Session()
                credentials = session.get_credentials()
                
                if credentials:
                    logger.info(f"AWS credentials found - Access Key ID: {credentials.access_key[:4]}****")
                else:
                    logger.warning("No AWS credentials found!")
                    
                region = os.environ.get("AWS_REGION", "us-east-1")
                logger.info(f"AWS Region: {region}")
                
                logger.info("Creating AWSV4SignerAuth...")
                auth = AWSV4SignerAuth(credentials, region)
                logger.info("AWS authentication configured")
                
                logger.info(f"Attempting to create OpenSearch client...")
                client = OpenSearch(
                    hosts=[{'host': os_host, 'port': 443}],
                    http_auth=auth,
                    use_ssl=True,
                    verify_certs=True,
                    ca_certs='/usr/local/share/ca-certificates/McK_Entrust_Root_G1.crt',
                    connection_class=RequestsHttpConnection,
                    timeout=timeout
                )
                logger.info(f"OpenSearch client created successfully for {os_host}:443")
                
                # Test connection with a simple info call
                logger.info("Testing connection with cluster info request...")
                info = client.info()
                logger.info(f"Connection test successful! Cluster name: {info.get('cluster_name', 'unknown')}, "
                          f"Version: {info.get('version', {}).get('number', 'unknown')}")
                logger.info("=" * 60)
                return client
            except Exception as e:
                logger.error(f"Failed to create or connect to OpenSearch client: {str(e)}")
                logger.error(f"Exception type: {type(e).__name__}")
                logger.error(f"Traceback:\n{traceback.format_exc()}")
                logger.info("=" * 60)
                raise

    def create_index(self, os_client, INDEX_NAME):
        index_exists = os_client.indices.exists(index=INDEX_NAME)
        print(f"creating {INDEX_NAME} index : exists={index_exists}")
        if not index_exists:
            # Load settings to include non-updateable settings during creation
            settings_file = os.path.join(self.base_dir, 'schema', INDEX_NAME, 'settings.json')
            settings = {}
            if os.path.exists(settings_file):
                with open(settings_file) as f:
                    settings = json.load(f)
                # Remove synonyms and stopwords as they'll be updated later
                # Also remove phonetic analyzer/filter if plugin not available
                if "analysis" in settings:
                    if "analyzer" in settings["analysis"]:
                        if "phonetic_analyzer" in settings["analysis"]["analyzer"]:
                            del settings["analysis"]["analyzer"]["phonetic_analyzer"]
                            print("Removing phonetic_analyzer from index creation (phonetic plugin may not be available)")
                    
                    if "filter" in settings["analysis"]:
                        if "metaphone_filter" in settings["analysis"]["filter"]:
                            del settings["analysis"]["filter"]["metaphone_filter"]
                            print("Removing metaphone_filter from index creation (phonetic plugin may not be available)")
                        if "synonym_filter" in settings["analysis"]["filter"]:
                            settings["analysis"]["filter"]["synonym_filter"]["synonyms"] = []
                        if "stop_filter" in settings["analysis"]["filter"]:
                            settings["analysis"]["filter"]["stop_filter"]["stopwords"] = []
            
            if settings:
                os_client.indices.create(index=INDEX_NAME, body={"settings": settings})
            else:
                os_client.indices.create(index=INDEX_NAME)
        print(f"creating {INDEX_NAME} index  ended.")
        # closing the index.
        print(f"closing the {INDEX_NAME} index.")
        response = os_client.indices.close(index=INDEX_NAME)
        print(response)
        print(f"closing the  {INDEX_NAME} index ended.")

    def update(self, os_client, INDEX_NAME):
            # Updating the synonyms.
            print(f"updating synonyms for {INDEX_NAME}.")
            with open(os.path.join(self.base_dir, 'schema', INDEX_NAME, 'synonyms.txt')) as file:
                synonyms = [line.rstrip('\n') for line in file]
            print(f"updating synonyms for {INDEX_NAME} ended.")

            # Updating the stopwords.
            print(f"updating stopwords for {INDEX_NAME} index.")
            with open(os.path.join(self.base_dir, 'schema', INDEX_NAME, 'stopwords.txt')) as file:
                stopwords = []
                for line in file:
                    for word in line.rstrip('\n').split(','):
                        stopwords.append(word.strip())
            print(f"updating stopwords for {INDEX_NAME} ended.")

            # Updating the settings.json
            print("updating settings started.")
            with open(os.path.join(self.base_dir, 'schema', INDEX_NAME, 'settings.json')) as file:
                try:
                    print(f"processing settings... for {INDEX_NAME} index.")
                    settings = json.load(file)
                    
                    # Filter out non-updateable settings (these can only be set at index creation)
                    non_updateable_settings = [
                        'index.knn',
                        'index.number_of_shards',
                        'index.codec',
                        'index.mapping.single_type',
                        'index.soft_deletes.enabled',
                        'index.hidden'
                    ]
                    
                    # Create a deep copy with only updateable settings to avoid modifying original
                    updateable_settings = {}
                    for key, value in settings.items():
                        if key not in non_updateable_settings:
                            updateable_settings[key] = copy.deepcopy(value)
                        else:
                            print(f"Skipping non-updateable setting: {key}")
                    
                    # Update analysis filters if they exist
                    if "analysis" in updateable_settings:
                        # Remove phonetic analyzer FIRST (before its referenced filter)
                        # This prevents errors when the analyzer references a filter that's being removed
                        if "analyzer" in updateable_settings["analysis"]:
                            if "phonetic_analyzer" in updateable_settings["analysis"]["analyzer"]:
                                del updateable_settings["analysis"]["analyzer"]["phonetic_analyzer"]
                                print("Removing phonetic_analyzer (phonetic plugin not available)")
                        
                        # Remove metaphone_filter AFTER removing the analyzer that references it
                        if "filter" in updateable_settings["analysis"]:
                            if "metaphone_filter" in updateable_settings["analysis"]["filter"]:
                                del updateable_settings["analysis"]["filter"]["metaphone_filter"]
                                print("Removing metaphone_filter (phonetic plugin not available)")
                            
                            # Update synonyms and stopwords
                            if "synonym_filter" in updateable_settings["analysis"]["filter"]:
                                updateable_settings["analysis"]["filter"]["synonym_filter"]["synonyms"] = synonyms
                            if "stop_filter" in updateable_settings["analysis"]["filter"]:
                                updateable_settings["analysis"]["filter"]["stop_filter"]["stopwords"] = stopwords
                    
                    response = os_client.indices.put_settings(body=updateable_settings, index=INDEX_NAME)
                    print(response)
                    if not response['acknowledged']:
                        exit(1)
                except Exception as e:
                    self.handleException(e)

                finally:
                    print(f"opening the {INDEX_NAME} index.")
                    response = os_client.indices.open(index=INDEX_NAME)
                    print(response)
            print(f"updating settings for {INDEX_NAME} ended.")

            # Updating the mappings.json
            print(f"updating mappings started for {INDEX_NAME} index.")
            with open(os.path.join(self.base_dir, 'schema', INDEX_NAME, 'mapping.json')) as f:
                try:
                    mapping = json.load(f)
                    
                    # Remove fields that reference phonetic_analyzer (requires plugin)
                    def remove_phonetic_refs(obj):
                        """Recursively remove 'ph' fields that use phonetic_analyzer"""
                        if isinstance(obj, dict):
                            result = {}
                            for key, value in obj.items():
                                # Check if this is a fields dict and remove 'ph' key
                                if key == 'fields' and isinstance(value, dict) and 'ph' in value:
                                    cleaned_fields = {k: remove_phonetic_refs(v) for k, v in value.items() if k != 'ph'}
                                    # Only include fields if it's not empty
                                    if cleaned_fields:
                                        result[key] = cleaned_fields
                                else:
                                    result[key] = remove_phonetic_refs(value)
                            return result
                        elif isinstance(obj, list):
                            return [remove_phonetic_refs(item) for item in obj]
                        else:
                            return obj
                    
                    cleaned_mapping = remove_phonetic_refs(mapping)
                    print("Removed phonetic_analyzer references ('ph' fields) from mappings")
                    
                    response = os_client.indices.put_mapping(body=cleaned_mapping, index=INDEX_NAME)
                    print(response)
                    if not response['acknowledged']:
                        exit(1)
                except Exception as e:
                    self.handleException(e)
            print(f"updating mappings ended for {INDEX_NAME}.")

            # Updating the scripts to normalize scores.
            print(f"updating scripts started for {INDEX_NAME}.")
            scripts_dir = os.path.join(self.base_dir, 'scripts', INDEX_NAME)
            for script_name in [file_name.replace(".json", "") for file_name in os.listdir(scripts_dir) if
                                ".json" in file_name]:
                with open(os.path.join(scripts_dir, f'{script_name}.json')) as f:
                    try:
                        print("processing " + script_name + " for " + INDEX_NAME + "} script...")
                        script_body = json.dumps(json.load(f))
                        response = os_client.put_script(id=script_name, body=script_body)
                        print(response)
                        if not response['acknowledged']:
                            exit(1)
                    except Exception as e:
                        self.handleException(e)
            print(f"updating scripts ended for {INDEX_NAME}.")

            # Updating the query templates.
            print(f"updating templates started for {INDEX_NAME} .")
            templates_dir = os.path.join(self.base_dir, 'templates')
            for query_template_name in [file_name.replace(".json", "") for file_name in os.listdir(templates_dir) if
                                        ".json" in file_name]:
                with open(os.path.join(templates_dir, f'{query_template_name}.json')) as f:
                    try:
                        print("processing " + query_template_name + " template... for " + INDEX_NAME + "index ")

                        template = json.load(f)

                        body = {
                            "script": {
                                "lang": "mustache",
                                "index": INDEX_NAME,
                                "source": json.dumps(template),
                            }
                        }

                        body_text = json.dumps(body)

                        json_field_mapping = [
                            [INDEX_NAME, "QUERY_VECTOR"],
                            [INDEX_NAME, "QUERY_FILTERS"],
                            [INDEX_NAME, "QUERY_MUSTS"],
                            [INDEX_NAME, "QUERY_SHOULDS"],
                            [INDEX_NAME, "QUERY_FMNOS"],
                            [INDEX_NAME, "QUERY_WORDS"],
                        ]

                        for mapping in json_field_mapping:
                            if INDEX_NAME == mapping[0]:
                                body_text = body_text.replace(
                                    '\\\"' + mapping[1] + '\\\"',
                                    '{{#toJson}}' + mapping[1] + '{{/toJson}}'
                                )

                        response = os_client.put_script(id=query_template_name, body=body_text)
                        print(response)
                        if not response['acknowledged']:
                            exit(1)
                    except Exception as e:
                        self.handleException(e)
            print(f"updating templates ended for {INDEX_NAME}.")

            print(f"opening the index for {INDEX_NAME}")
            response = os_client.indices.open(index=INDEX_NAME)
            print(response)
            print(f"opening the index ended for {INDEX_NAME}.")

    def migrate(self):
        for index_name in ["expertise"]:
            # INDEX_NAME = os.environ.get("INDEX_NAME")
            os_client = self.opensearch_client()
            self.create_index(os_client, index_name)
            self.update(os_client, index_name)

    def handleException(self, e):
        print(e)
        print(traceback.format_exc())
        exit(1)

if __name__ == '__main__':
    migrations = Migrations()
    migrations.migrate()