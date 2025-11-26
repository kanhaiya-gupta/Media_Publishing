"""
API Extractor
=============

Extract data from external APIs.
"""

from typing import Optional, Dict, Any, List
from pyspark.sql import DataFrame, SparkSession
import requests
import json
import logging

from ..base.extractor import BaseExtractor

logger = logging.getLogger(__name__)


class APIExtractor(BaseExtractor):
    """
    Extract data from external APIs.
    
    Supports:
    - REST API endpoints
    - Pagination
    - Authentication (Bearer token, API key, Basic auth)
    - Rate limiting
    """
    
    def __init__(
        self,
        spark: SparkSession,
        extractor_config: Optional[Dict[str, Any]] = None
    ):
        """
        Initialize API extractor.
        
        Args:
            spark: SparkSession instance
            extractor_config: Extractor-specific configuration
                - base_url: Base API URL
                - auth_type: Authentication type ("bearer", "apikey", "basic", None)
                - auth_token: Authentication token
                - headers: Additional headers
                - timeout: Request timeout (seconds)
        """
        super().__init__(spark, extractor_config)
        self.base_url = self.config.get("base_url", "")
        self.auth_type = self.config.get("auth_type")
        self.auth_token = self.config.get("auth_token")
        self.headers = self.config.get("headers", {})
        self.timeout = self.config.get("timeout", 30)
    
    def _get_headers(self) -> Dict[str, str]:
        """Get request headers with authentication."""
        headers = self.headers.copy()
        
        if self.auth_type == "bearer" and self.auth_token:
            headers["Authorization"] = f"Bearer {self.auth_token}"
        elif self.auth_type == "apikey" and self.auth_token:
            api_key_name = self.config.get("api_key_name", "X-API-Key")
            headers[api_key_name] = self.auth_token
        elif self.auth_type == "basic" and self.auth_token:
            headers["Authorization"] = f"Basic {self.auth_token}"
        
        return headers
    
    def extract(
        self,
        endpoint: str,
        method: str = "GET",
        params: Optional[Dict[str, Any]] = None,
        pagination: bool = False,
        page_size: int = 100,
        max_pages: Optional[int] = None,
        **kwargs
    ) -> DataFrame:
        """
        Extract data from API endpoint.
        
        Args:
            endpoint: API endpoint path
            method: HTTP method ("GET", "POST", etc.)
            params: Query parameters
            pagination: Whether to handle pagination
            page_size: Number of items per page
            max_pages: Maximum number of pages to fetch (None for all)
            **kwargs: Additional request parameters
            
        Returns:
            Spark DataFrame with extracted data
        """
        try:
            url = f"{self.base_url}/{endpoint.lstrip('/')}"
            headers = self._get_headers()
            
            all_data = []
            page = 1
            
            self.logger.info(f"Extracting data from API: {url}")
            
            while True:
                request_params = params.copy() if params else {}
                
                if pagination:
                    request_params["page"] = page
                    request_params["page_size"] = page_size
                
                response = requests.request(
                    method=method,
                    url=url,
                    params=request_params,
                    headers=headers,
                    timeout=self.timeout,
                    **kwargs
                )
                
                response.raise_for_status()
                data = response.json()
                
                # Handle different response formats
                if isinstance(data, list):
                    items = data
                elif isinstance(data, dict):
                    # Common pagination patterns
                    if "data" in data:
                        items = data["data"]
                    elif "results" in data:
                        items = data["results"]
                    elif "items" in data:
                        items = data["items"]
                    else:
                        items = [data]
                else:
                    items = [data]
                
                all_data.extend(items)
                
                # Check if more pages exist
                if pagination:
                    if isinstance(data, dict):
                        has_next = data.get("has_next", False) or data.get("next", None) is not None
                        if not has_next:
                            break
                    else:
                        # If response is a list, assume no pagination
                        break
                    
                    page += 1
                    if max_pages and page > max_pages:
                        break
                else:
                    break
            
            if not all_data:
                self.logger.warning("No data extracted from API")
                return self.spark.createDataFrame([], schema=None)
            
            # Convert to DataFrame
            df = self.spark.createDataFrame(all_data)
            
            self.log_extraction_stats(df, f"API: {url}")
            return df
            
        except Exception as e:
            self.logger.error(f"Error extracting data from API: {e}", exc_info=True)
            raise

