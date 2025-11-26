"""
Python Callable Operator
========================

Airflow operator for executing Python callables (functions).
"""

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from typing import Dict, Any, Optional, Callable
import logging
import inspect

logger = logging.getLogger(__name__)


class PythonCallableOperator(BaseOperator):
    """
    Operator for executing Python callables (functions).
    
    Can import and call any Python function directly.
    """
    
    template_fields = ("op_kwargs",)
    
    @apply_defaults
    def __init__(
        self,
        python_callable: Callable,
        op_kwargs: Optional[Dict[str, Any]] = None,
        *args,
        **kwargs
    ):
        """
        Initialize Python callable operator.
        
        Args:
            python_callable: Python function to call
            op_kwargs: Arguments to pass to the function
            *args: Additional operator arguments
            **kwargs: Additional operator arguments
        """
        super().__init__(*args, **kwargs)
        self.python_callable = python_callable
        self.op_kwargs = op_kwargs or {}
        self.logger = logging.getLogger(f"{__name__}.PythonCallableOperator")
    
    def execute(self, context: Dict[str, Any]) -> Any:
        """
        Execute the Python callable.
        
        Args:
            context: Airflow context dictionary
            
        Returns:
            Result from the callable
        """
        try:
            self.logger.info(f"Executing Python callable: {self.python_callable.__name__}")
            
            # Get function signature to check which parameters it accepts
            sig = inspect.signature(self.python_callable)
            accepted_params = set(sig.parameters.keys())
            
            # Start with op_kwargs
            merged_kwargs = {**self.op_kwargs}
            
            # Only add context variables if the function accepts them
            if "execution_date" in accepted_params and "ds" in context:
                merged_kwargs["execution_date"] = context["ds"]
            
            if "data_interval_start" in accepted_params and "data_interval_start" in context:
                merged_kwargs["data_interval_start"] = context["data_interval_start"]
            
            if "data_interval_end" in accepted_params and "data_interval_end" in context:
                merged_kwargs["data_interval_end"] = context["data_interval_end"]
            
            # Filter out any kwargs that the function doesn't accept
            filtered_kwargs = {k: v for k, v in merged_kwargs.items() if k in accepted_params}
            
            # Call the function
            self.logger.info(f"Calling function with kwargs: {filtered_kwargs}")
            result = self.python_callable(**filtered_kwargs)
            
            self.logger.info(f"Function completed successfully: {self.python_callable.__name__}")
            return result
                
        except Exception as e:
            self.logger.error(f"Error executing callable: {e}", exc_info=True)
            raise
        finally:
            # Always clean up Spark sessions after execution
            # This ensures resources are properly released in Airflow
            try:
                from src.ownlens.processing.etl.utils.spark_session import SparkSessionManager
                SparkSessionManager.stop_session()
                self.logger.info("✅ Spark sessions cleaned up after task completion")
            except ImportError:
                # Spark session manager not available (e.g., for non-ETL tasks)
                pass
            except Exception as e:
                self.logger.warning(f"⚠️  Error cleaning up Spark sessions: {e}")
