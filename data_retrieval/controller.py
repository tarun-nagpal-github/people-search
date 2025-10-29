import os
import logging
from typing import Optional, List, Dict, Any
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse
import boto3
from opensearchpy import OpenSearch, RequestsHttpConnection, AWSV4SignerAuth

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Initialize FastAPI app
app = FastAPI(
    title="People Search API",
    description="API for searching people data in OpenSearch",
    version="1.0.0"
)


def opensearch_client():
    """Create and return an OpenSearch client."""
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


def build_keyword_query(keyword: str, fields: List[str] = None) -> Dict[str, Any]:
    """
    Build a multi-match query for keyword search across multiple fields.
    
    Args:
        keyword: The search keyword
        fields: List of fields to search in. If None, searches across common text fields.
    
    Returns:
        Dict containing the query DSL
    """
    if fields is None:
        # Default fields to search across
        fields = [
            "name.first^2",  # Boost name.first field
            "name.last^2",   # Boost name.last field
            "name.first.ph",  # Phonetic match for first name
            "name.last.ph",   # Phonetic match for last name
            "email",
            "location.city",
            "location.state",
            "location.country",
            "location.street.name",
            "phone",
            "cell",
            "nat",
            "gender"
        ]
    
    return {
        "multi_match": {
            "query": keyword,
            "fields": fields,
            "type": "best_fields",
            "operator": "or",
            "fuzziness": "AUTO"  # Enable fuzzy matching for typos
        }
    }


@app.get("/")
async def root():
    """Root endpoint with API information."""
    return {
        "message": "People Search API",
        "version": "1.0.0",
        "endpoints": {
            "search": "/search?keyword=<your_keyword>",
            "health": "/health"
        }
    }


@app.get("/health")
async def health_check():
    """Health check endpoint to verify OpenSearch connection."""
    try:
        client = opensearch_client()
        info = client.info()
        return {
            "status": "healthy",
            "opensearch": {
                "cluster_name": info.get('cluster_name', 'N/A'),
                "version": info.get('version', {}).get('number', 'N/A')
            }
        }
    except Exception as e:
        logger.error(f"Health check failed: {str(e)}")
        raise HTTPException(status_code=503, detail=f"OpenSearch connection failed: {str(e)}")


@app.get("/search")
async def search_people(
    keyword: str = Query(..., description="Keyword to search for"),
    index_name: str = Query("people", description="OpenSearch index name"),
    page: int = Query(1, ge=1, description="Page number (starts from 1)"),
    size: int = Query(10, ge=1, le=100, description="Number of results per page"),
    fields: Optional[str] = Query(None, description="Comma-separated list of fields to search (optional)")
) -> JSONResponse:
    """
    Search for people based on keyword.
    
    This endpoint searches across multiple fields including name, email, location, etc.
    
    Args:
        keyword: The search keyword
        index_name: The OpenSearch index to search in (default: "people")
        page: Page number for pagination (default: 1)
        size: Number of results per page (default: 10, max: 100)
        fields: Optional comma-separated list of specific fields to search
    
    Returns:
        JSON response with search results
    """
    try:
        # Calculate from parameter for pagination
        from_param = (page - 1) * size
        
        # Parse fields if provided
        search_fields = None
        if fields:
            search_fields = [f.strip() for f in fields.split(",")]
        
        # Build the search query
        query = build_keyword_query(keyword, search_fields)
        
        # Build the complete search body
        search_body = {
            "from": from_param,
            "size": size,
            "query": query,
            "_source": {
                "includes": [
                    "gender",
                    "name.*",
                    "location.*",
                    "email",
                    "login.*",
                    "dob.*",
                    "registered.*",
                    "phone",
                    "cell",
                    "id.*",
                    "picture.*",
                    "nat"
                ]
            },
            "highlight": {
                "fields": {
                    "name.first": {},
                    "name.last": {},
                    "email": {},
                    "location.city": {},
                    "location.state": {},
                    "location.country": {}
                }
            }
        }
        
        # Execute search
        client = opensearch_client()
        response = client.search(
            index=index_name,
            body=search_body
        )
        
        # Extract results
        hits = response.get('hits', {})
        total = hits.get('total', {})
        
        # Handle different total formats (int or dict with value)
        if isinstance(total, dict):
            total_hits = total.get('value', 0)
        else:
            total_hits = total
        
        results = []
        for hit in hits.get('hits', []):
            result = {
                "id": hit.get('_id'),
                "score": hit.get('_score'),
                "source": hit.get('_source', {}),
                "highlight": hit.get('highlight', {})
            }
            results.append(result)
        
        # Calculate pagination info
        total_pages = (total_hits + size - 1) // size if total_hits > 0 else 0
        
        return JSONResponse({
            "success": True,
            "keyword": keyword,
            "total_hits": total_hits,
            "page": page,
            "size": size,
            "total_pages": total_pages,
            "results": results
        })
        
    except Exception as e:
        logger.error(f"Search failed: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Search failed: {str(e)}")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)

