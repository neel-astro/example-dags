"""
repro dag for https://github.com/astronomer/astro-agent/issues/595
https://astronomer.zendesk.com/agent/tickets/76634
courtesy of @karenbraganz

Updated to use custom operator with operator link for xcom handling
"""

from typing import Any, Dict, List, Optional
from urllib.parse import urlencode

from pendulum import datetime
from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.models.baseoperatorlink import BaseOperatorLink
from airflow.utils.context import Context


class XComOperatorLink(BaseOperatorLink):
    """Custom operator link for XCom operations with enhanced viewing capabilities."""
    
    name = "XCom Values"

    def get_link(self, operator: "PythonOperator", *, ti_key: "TaskInstanceKey") -> str:
        """
        Generate a link to view XCom values for this task instance.
        
        Args:
            operator: The operator instance
            ti_key: TaskInstance key containing dag_id, task_id, run_id, etc.
            
        Returns:
            URL string to view XCom values
        """
        # In a real implementation, this would link to your Airflow UI or custom dashboard
        # For demo purposes, we'll create a parameterized link
        params = {
            'dag_id': ti_key.dag_id,
            'task_id': ti_key.task_id,
            'execution_date': ti_key.run_id,
        }
        base_url = "s3://default@neel-test-s3/xcom/admin/airflow/xcom"
        return f"{base_url}?{urlencode(params)}"

    @staticmethod
    def persist(context: Context) -> None:
        context["ti"].xcom_push(
            key="query_id",
            value=datetime.now("UTC").to_iso8601_string(),
        )

def xcom_operation_function(
    operation_type: str,
    input_value: Optional[Any] = None,
    xcom_key: str = 'return_value',
    operation_params: Optional[Dict[str, Any]] = None,
    upstream_task_ids: Optional[List[str]] = None,
    **context
) -> Any:
    """
    Function to handle XCom operations for the PythonOperator.
    
    Args:
        operation_type: Type of operation ('store', 'retrieve', or 'process')
        input_value: Value to store or process
        xcom_key: Key to use for XCom storage/retrieval
        operation_params: Additional parameters for the operation
        upstream_task_ids: Task IDs to retrieve XCom values from
        context: Airflow task context (automatically passed)
        
    Returns:
        Result of the operation
    """
    ti = context['ti']
    
    if operation_type == 'store':
        print(f"Storing value to XCom: {input_value}")
        ti.xcom_push(key=xcom_key, value=input_value)
        return input_value
        
    elif operation_type == 'retrieve':
        retrieved_values = {}
        for task_id in (upstream_task_ids or []):
            value = ti.xcom_pull(task_ids=task_id, key=xcom_key)
            retrieved_values[task_id] = value
            print(f"Retrieved from {task_id}: {value}")
        
        if len(upstream_task_ids or []) == 1:
            return retrieved_values[upstream_task_ids[0]]
        return retrieved_values
        
    elif operation_type == 'process':
        # Get the input value
        if input_value is not None:
            value = input_value
        elif upstream_task_ids:
            if len(upstream_task_ids) == 1:
                value = ti.xcom_pull(task_ids=upstream_task_ids[0], key=xcom_key)
            else:
                value = [ti.xcom_pull(task_ids=task_id, key=xcom_key) 
                        for task_id in upstream_task_ids]
        else:
            raise ValueError("No input value or upstream tasks specified for process operation")
        
        print(f"Processing value: {value}")
        
        # Apply processing based on operation_params
        operation_params = operation_params or {}
        if 'multiply' in operation_params:
            result = value * operation_params['multiply']
        elif 'transform_type' in operation_params:
            # Use string-based transformation to avoid lambda pickle issues
            transform_type = operation_params['transform_type']
            if transform_type == 'add_prefix':
                prefix = operation_params.get('prefix', 'processed_')
                result = f"{prefix}{value}"
            else:
                result = value
        else:
            result = value
        
        print(f"Processed result: {result}")
        return result
    
    else:
        raise ValueError(f"Unknown operation_type: {operation_type}")


def retrieve_all_xcom_values(**context) -> Dict[str, Any]:
    """
    Custom function for task 4 to retrieve all XCom values from previous tasks.
    Uses ti.xcom_pull(task_ids) to get all values at once.
    
    Args:
        context: Airflow task context (automatically passed)
        
    Returns:
        Dictionary containing all retrieved XCom values
    """
    ti = context['ti']
    
    # Get all previous task IDs
    all_task_ids = [
        'task_1',
        'task_2_0', 'task_2_1', 'task_2_2',
        'task_3_0', 'task_3_1', 'task_3_2'
    ]
    
    print(f"Retrieving XCom values from tasks: {all_task_ids}")
    
    # Use ti.xcom_pull with task_ids list to get all values at once
    all_xcom_values = ti.xcom_pull(task_ids=all_task_ids)
    
    print("Retrieved XCom values:")
    for task_id, value in zip(all_task_ids, all_xcom_values):
        print(f"  {task_id}: {value}")
    
    # Return as a dictionary for easier access
    result = dict(zip(all_task_ids, all_xcom_values))
    return result


class XComOperator(PythonOperator):
    """
    Airflow 3 compliant custom operator for handling XCom operations.
    Built on top of PythonOperator for maximum compatibility.
    """
    
    # Airflow 3 compatible operator links
    operator_extra_links = (XComOperatorLink(),)
    
    def __init__(
        self,
        operation_type: str = 'store',
        input_value: Optional[Any] = None,
        xcom_key: str = 'return_value',
        operation_params: Optional[Dict[str, Any]] = None,
        upstream_task_ids: Optional[List[str]] = None,
        **kwargs
    ):
        """
        Initialize the XComOperator.
        
        Args:
            operation_type: Type of operation ('store', 'retrieve', or 'process')
            input_value: Value to store or process
            xcom_key: Key to use for XCom storage/retrieval
            operation_params: Additional parameters for the operation
            upstream_task_ids: Task IDs to retrieve XCom values from
        """
        # Set up the python_callable and op_kwargs for PythonOperator
        kwargs['python_callable'] = xcom_operation_function
        kwargs['op_kwargs'] = {
            'operation_type': operation_type,
            'input_value': input_value,
            'xcom_key': xcom_key,
            'operation_params': operation_params,
            'upstream_task_ids': upstream_task_ids,
        }
        
        super().__init__(**kwargs)


# Define the DAG
dag = DAG(
    'dynamic_xcom_custom_operator',
    start_date=datetime(2024, 10, 1),
    schedule=None,  # Airflow 3 uses 'schedule' instead of 'schedule_interval'
    catchup=False,
    description='Dynamic XCom DAG using custom operator with operator links',
    tags=["xcom", "custom-operator", "airflow-3"]
)

# Task 1: Initial task (store operation)
task_1 = XComOperator(
    task_id='task_1',
    operation_type='store',
    input_value="task_1_completed",
    dag=dag
)

# Task 2: Process values (expand with different input values)
task_2_configs = []
for i in [0, 1, 2]:
    task_2_configs.append(
        XComOperator(
            task_id=f'task_2_{i}',
            operation_type='process',
            input_value=i,
            operation_params={'multiply': 10},  # Each value will be multiplied by 10
            dag=dag
        )
    )

# Task 3: Process outputs from task_2 (expand based on task_2 outputs)
task_3_configs = []
for i, task_2_instance in enumerate(task_2_configs):
    task_3_configs.append(
        XComOperator(
            task_id=f'task_3_{i}',
            operation_type='process',
            upstream_task_ids=[task_2_instance.task_id],
            operation_params={
                'transform_type': 'add_prefix',
                'prefix': 'processed_'
            },
            dag=dag
        )
    )

# Task 4: Retrieve values from all previous tasks using ti.xcom_pull(task_ids)
task_4 = PythonOperator(
    task_id='task_4',
    python_callable=retrieve_all_xcom_values,
    dag=dag
)

# Set up dependencies - task_1 to all task_2 instances
for task_2 in task_2_configs:
    task_1 >> task_2

# Connect each task_2 instance to corresponding task_3 instance
for task_2, task_3 in zip(task_2_configs, task_3_configs):
    task_2 >> task_3

# Connect all previous tasks to task_4
task_1 >> task_4
for task_2 in task_2_configs:
    task_2 >> task_4
for task_3 in task_3_configs:
    task_3 >> task_4
