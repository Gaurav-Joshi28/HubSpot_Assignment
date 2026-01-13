"""
================================================================================
FILE: dbt_rental_property_dag.py
================================================================================

PURPOSE:
    Airflow DAG to orchestrate the dbt rental property analytics pipeline.
    Executes models sequentially in the correct dependency order.
    
    Includes source freshness check before pipeline execution.

SCHEDULE:
    Daily at 6:00 AM UTC

MONITORING:
    - Access Airflow UI: http://localhost:8080
    - View task logs, run history, and execution times
    - Email alerts for failures and stale sources

LAYERS EXECUTED:
    0. Source Freshness Check → Verify raw data is updated
    1. Staging     → Clean and standardize raw data
    2. Intermediate → Business transformations & SCD
    3. Marts       → Dimensions and Fact tables
    4. Analytics   → Business question answers
    5. Tests       → Data quality validation

================================================================================
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
from airflow.exceptions import AirflowSkipException
import subprocess
import json
import os


# =============================================================================
# DAG Configuration
# =============================================================================

default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'email': ['data-alerts@company.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=1),
}

# dbt project directory
DBT_PROJECT_DIR = '/opt/dbt'

# Base dbt command
DBT_CMD = f'cd {DBT_PROJECT_DIR} && dbt'


# =============================================================================
# Source Freshness Check Functions
# =============================================================================

def check_schema_changes(**context):
    """
    Check if new amenity columns have been added to source.
    Determines if full refresh is needed instead of incremental.
    
    Returns:
        str: 'schema_changed' or 'schema_unchanged'
    """
    import subprocess
    import json
    
    # Run dbt operation to check for schema changes
    result = subprocess.run(
        f'cd {DBT_PROJECT_DIR} && dbt run-operation check_amenity_schema_change --output json',
        shell=True,
        capture_output=True,
        text=True
    )
    
    # Parse output to determine if schema changed
    output = result.stdout + result.stderr
    
    if 'SCHEMA CHANGE DETECTED' in output:
        context['ti'].xcom_push(key='refresh_strategy', value='full_refresh')
        return 'schema_changed'
    else:
        context['ti'].xcom_push(key='refresh_strategy', value='incremental')
        return 'schema_unchanged'


def check_late_arrivals(**context):
    """
    Detect late-arriving rows (older business dates but recently ingested)
    and decide if a backfill/full refresh is needed.
    """
    import subprocess
    import re
    
    cmd = (
        f'cd {DBT_PROJECT_DIR} && '
        f'dbt run-operation check_late_arrivals '
        f'--args \'{{"window_days": 7, "lookback_loaded_hours": 48}}\''
    )
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    output = result.stdout + result.stderr
    
    late_count = 0
    match = re.search(r'LATE_ARRIVAL_COUNT=(\\d+)', output)
    if match:
        late_count = int(match.group(1))
    
    context['ti'].xcom_push(key='late_arrivals_detected', value=(late_count > 0))
    context['ti'].xcom_push(key='late_arrival_count', value=late_count)
    
    if late_count > 0:
        return 'late_arrivals_detected'
    else:
        return 'late_arrivals_clean'


def check_source_freshness(**context):
    """
    Run dbt source freshness and check if sources are stale.
    
    Returns:
        dict: Freshness check results with status and details
    """
    import subprocess
    import json
    
    # Run dbt source freshness
    result = subprocess.run(
        f'cd {DBT_PROJECT_DIR} && dbt source freshness --output json',
        shell=True,
        capture_output=True,
        text=True
    )
    
    # Parse results
    freshness_results = {
        'passed': [],
        'warned': [],
        'errored': [],
        'runtime_error': None
    }
    
    if result.returncode != 0:
        # Check if it's a runtime error or just freshness failures
        if 'Runtime Error' in result.stderr:
            freshness_results['runtime_error'] = result.stderr
            context['ti'].xcom_push(key='freshness_results', value=freshness_results)
            return 'source_freshness_failed'
    
    # Try to parse the JSON output
    try:
        # Read the freshness output from target directory
        freshness_file = f'{DBT_PROJECT_DIR}/target/sources.json'
        if os.path.exists(freshness_file):
            with open(freshness_file, 'r') as f:
                sources_data = json.load(f)
                
            for source in sources_data.get('results', []):
                source_name = f"{source.get('unique_id', 'unknown')}"
                status = source.get('status', 'unknown')
                max_loaded_at = source.get('max_loaded_at', 'N/A')
                
                source_info = {
                    'name': source_name,
                    'max_loaded_at': max_loaded_at,
                    'status': status
                }
                
                if status == 'pass':
                    freshness_results['passed'].append(source_info)
                elif status == 'warn':
                    freshness_results['warned'].append(source_info)
                elif status == 'error':
                    freshness_results['errored'].append(source_info)
                    
    except Exception as e:
        freshness_results['runtime_error'] = str(e)
    
    # Push results to XCom for downstream tasks
    context['ti'].xcom_push(key='freshness_results', value=freshness_results)
    
    # Determine next task based on results
    if freshness_results['errored']:
        return 'source_freshness_failed'
    elif freshness_results['warned']:
        return 'source_freshness_warning'
    else:
        return 'source_freshness_passed'


def handle_freshness_warning(**context):
    """
    Handle source freshness warning - log warning but continue pipeline.
    """
    ti = context['ti']
    results = ti.xcom_pull(key='freshness_results', task_ids='check_source_freshness')
    
    warned_sources = results.get('warned', [])
    
    warning_message = f"""
    ⚠️ SOURCE FRESHNESS WARNING
    
    The following sources have not been updated recently:
    
    {chr(10).join([f"  - {s['name']}: Last loaded at {s['max_loaded_at']}" for s in warned_sources])}
    
    Pipeline will continue, but data may be stale.
    Please investigate the source data refresh process.
    """
    
    print(warning_message)
    
    # In production, you could send a Slack message or email here
    # from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
    

def handle_freshness_failure(**context):
    """
    Handle source freshness error - send alert and fail the pipeline.
    """
    ti = context['ti']
    results = ti.xcom_pull(key='freshness_results', task_ids='check_source_freshness')
    
    errored_sources = results.get('errored', [])
    runtime_error = results.get('runtime_error')
    
    error_message = f"""
    🚨 SOURCE FRESHNESS ERROR - PIPELINE BLOCKED
    
    The following sources have critically stale data:
    
    {chr(10).join([f"  - {s['name']}: Last loaded at {s['max_loaded_at']}" for s in errored_sources])}
    
    {f"Runtime Error: {runtime_error}" if runtime_error else ""}
    
    Pipeline execution has been STOPPED.
    Immediate action required to refresh source data.
    """
    
    print(error_message)
    
    # Raise exception to fail the task and trigger email alert
    raise Exception(error_message)


# =============================================================================
# DAG Definition
# =============================================================================

with DAG(
    dag_id='dbt_rental_property_pipeline',
    default_args=default_args,
    description='dbt pipeline for Rental Property Analytics with source freshness checks',
    schedule_interval='0 6 * * *',  # Daily at 6:00 AM UTC
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['dbt', 'rental-property', 'analytics', 'source-freshness'],
    doc_md=__doc__,
) as dag:

    # =========================================================================
    # Start Task
    # =========================================================================
    
    start = EmptyOperator(task_id='start')

    # =========================================================================
    # Source Freshness Check (BEFORE Pipeline)
    # =========================================================================
    
    check_source_freshness_task = BranchPythonOperator(
        task_id='check_source_freshness',
        python_callable=check_source_freshness,
        provide_context=True,
        doc_md="""
        Check if source tables have been refreshed recently.
        
        Uses dbt source freshness to verify:
        - raw.listings
        - raw.calendar
        - raw.generated_reviews
        - raw.amenities_changelog
        
        Branches to:
        - source_freshness_passed: All sources are fresh
        - source_freshness_warning: Some sources are stale (warn threshold)
        - source_freshness_failed: Critical staleness (error threshold)
        """,
    )
    
    # Freshness check outcomes
    source_freshness_passed = EmptyOperator(
        task_id='source_freshness_passed',
        doc_md='All sources are fresh - proceed with pipeline',
    )
    
    source_freshness_warning = PythonOperator(
        task_id='source_freshness_warning',
        python_callable=handle_freshness_warning,
        provide_context=True,
        doc_md='Some sources are stale - log warning and continue',
    )
    
    source_freshness_failed = PythonOperator(
        task_id='source_freshness_failed',
        python_callable=handle_freshness_failure,
        provide_context=True,
        trigger_rule='all_done',
        doc_md='Critical source staleness - fail pipeline and alert',
    )
    
    # Join point after freshness check
    freshness_check_complete = EmptyOperator(
        task_id='freshness_check_complete',
        trigger_rule='none_failed_min_one_success',
        doc_md='Freshness check complete - proceed with dbt pipeline',
    )

    # =========================================================================
    # dbt Setup Tasks
    # =========================================================================
    
    dbt_deps = BashOperator(
        task_id='dbt_deps',
        bash_command=f'{DBT_CMD} deps',
        doc_md='Install dbt package dependencies',
    )

    dbt_debug = BashOperator(
        task_id='dbt_debug',
        bash_command=f'{DBT_CMD} debug',
        doc_md='Verify dbt connection to Snowflake',
    )

    # =========================================================================
    # Layer 1: Staging Models
    # =========================================================================
    
    with TaskGroup(group_id='staging') as staging_group:
        
        stg_listings = BashOperator(
            task_id='stg_listings',
            bash_command=f'{DBT_CMD} run --select stg_listings',
        )
        
        stg_calendar = BashOperator(
            task_id='stg_calendar',
            bash_command=f'{DBT_CMD} run --select stg_calendar',
        )
        
        stg_reviews = BashOperator(
            task_id='stg_reviews',
            bash_command=f'{DBT_CMD} run --select stg_reviews',
        )
        
        stg_amenities_changelog = BashOperator(
            task_id='stg_amenities_changelog',
            bash_command=f'{DBT_CMD} run --select stg_amenities_changelog',
        )
        
        # Staging models can run in parallel
        [stg_listings, stg_calendar, stg_reviews, stg_amenities_changelog]

    # =========================================================================
    # Layer 2: Intermediate Models
    # =========================================================================
    
    with TaskGroup(group_id='intermediate') as intermediate_group:
        
        int_listing_amenities_scd = BashOperator(
            task_id='int_listing_amenities_scd',
            bash_command=f'{DBT_CMD} run --select int_listing_amenities_scd',
        )
        
        int_hosts_history = BashOperator(
            task_id='int_hosts_history',
            bash_command=f'{DBT_CMD} run --select int_hosts_history',
        )
        
        int_availability_spans = BashOperator(
            task_id='int_availability_spans',
            bash_command=f'{DBT_CMD} run --select int_availability_spans',
        )
        
        int_listings_history = BashOperator(
            task_id='int_listings_history',
            bash_command=f'{DBT_CMD} run --select int_listings_history',
        )
        
        int_calendar_enriched = BashOperator(
            task_id='int_calendar_enriched',
            bash_command=f'{DBT_CMD} run --select int_calendar_enriched',
        )
        
        # Define dependencies within intermediate layer
        int_listing_amenities_scd >> int_listings_history
        int_listing_amenities_scd >> int_calendar_enriched

    # =========================================================================
    # Schema Change Detection (Before Marts)
    # =========================================================================
    
    check_schema_changes_task = BranchPythonOperator(
        task_id='check_schema_changes',
        python_callable=check_schema_changes,
        provide_context=True,
        doc_md="""
        Checks if new amenity columns have been added to source data.
        
        If new amenities detected:
        - Full refresh required to backfill historical data
        - Branches to 'schema_changed' path
        
        If no changes:
        - Incremental run is safe
        - Branches to 'schema_unchanged' path
        """,
    )
    
    schema_changed = EmptyOperator(
        task_id='schema_changed',
        doc_md='New amenity columns detected - will run full refresh',
    )
    
    schema_unchanged = EmptyOperator(
        task_id='schema_unchanged',
        doc_md='No schema changes - incremental run',
    )
    
    schema_check_complete = EmptyOperator(
        task_id='schema_check_complete',
        trigger_rule='none_failed_min_one_success',
    )
    
    # =========================================================================
    # Late Arrival Detection (Before Marts)
    # =========================================================================
    
    check_late_arrivals_task = BranchPythonOperator(
        task_id='check_late_arrivals',
        python_callable=check_late_arrivals,
        provide_context=True,
        doc_md="""
        Detects late-arriving rows (older business dates but recently ingested).
        If detected, we will trigger a full refresh/backfill for the fact model.
        """,
    )
    
    late_arrivals_detected = EmptyOperator(
        task_id='late_arrivals_detected',
        doc_md='Late arrivals found - favor full refresh for correctness',
    )
    
    late_arrivals_clean = EmptyOperator(
        task_id='late_arrivals_clean',
        doc_md='No late arrivals - incremental path is safe',
    )
    
    late_arrivals_check_complete = EmptyOperator(
        task_id='late_arrivals_check_complete',
        trigger_rule='none_failed_min_one_success',
    )

    # =========================================================================
    # Layer 3: Mart Models (Dimensions & Facts)
    # =========================================================================
    
    with TaskGroup(group_id='marts') as marts_group:
        
        # Dimensions
        dim_date = BashOperator(
            task_id='dim_date',
            bash_command=f'{DBT_CMD} run --select dim_date',
        )
        
        dim_hosts = BashOperator(
            task_id='dim_hosts',
            bash_command=f'{DBT_CMD} run --select dim_hosts',
        )
        
        dim_listings = BashOperator(
            task_id='dim_listings',
            bash_command=f'{DBT_CMD} run --select dim_listings',
        )
        
        # Fact table with dynamic refresh strategy
        # Uses XCom to determine if full refresh is needed
        fct_daily = BashOperator(
            task_id='fct_daily_listing_performance',
            bash_command='''
                SCHEMA_STRATEGY="{{ ti.xcom_pull(key='refresh_strategy', task_ids='check_schema_changes') }}"
                LATE_ARRIVALS="{{ ti.xcom_pull(key='late_arrivals_detected', task_ids='check_late_arrivals') }}"
                if [ "$SCHEMA_STRATEGY" = "full_refresh" ] || [ "$LATE_ARRIVALS" = "True" ]; then
                    echo "🔄 Full refresh triggered (schema change or late arrivals)"
                    ''' + DBT_CMD + ''' run --full-refresh --select fct_daily_listing_performance
                else
                    echo "⚡ Incremental run"
                    ''' + DBT_CMD + ''' run --select fct_daily_listing_performance
                fi
            ''',
        )
        
        fct_monthly_listing = BashOperator(
            task_id='fct_monthly_listing_performance',
            bash_command=f'{DBT_CMD} run --select fct_monthly_listing_performance',
        )
        
        fct_monthly_neighborhood = BashOperator(
            task_id='fct_monthly_neighborhood_summary',
            bash_command=f'{DBT_CMD} run --select fct_monthly_neighborhood_summary',
        )
        
        # Define dependencies within marts layer
        [dim_date, dim_hosts] >> dim_listings
        dim_listings >> fct_daily
        fct_daily >> [fct_monthly_listing, fct_monthly_neighborhood]

    # =========================================================================
    # Layer 4: Analytics Models
    # =========================================================================
    
    with TaskGroup(group_id='analytics') as analytics_group:
        
        problem_1 = BashOperator(
            task_id='problem_1_amenity_revenue',
            bash_command=f'{DBT_CMD} run --select problem_1_amenity_revenue',
        )
        
        problem_2 = BashOperator(
            task_id='problem_2_neighborhood_pricing',
            bash_command=f'{DBT_CMD} run --select problem_2_neighborhood_pricing',
        )
        
        problem_3a = BashOperator(
            task_id='problem_3a_max_stay_duration',
            bash_command=f'{DBT_CMD} run --select problem_3a_max_stay_duration',
        )
        
        problem_3b = BashOperator(
            task_id='problem_3b_max_stay_lockbox_firstaid',
            bash_command=f'{DBT_CMD} run --select problem_3b_max_stay_lockbox_firstaid',
        )
        
        # Analytics can run in parallel
        [problem_1, problem_2, problem_3a, problem_3b]

    # =========================================================================
    # Layer 5: Data Quality Tests
    # =========================================================================
    
    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command=f'{DBT_CMD} test',
        doc_md='Run all dbt data quality tests',
    )

    # =========================================================================
    # Generate Documentation
    # =========================================================================
    
    dbt_docs = BashOperator(
        task_id='dbt_docs_generate',
        bash_command=f'{DBT_CMD} docs generate',
        doc_md='Generate dbt documentation',
    )

    # =========================================================================
    # End Task
    # =========================================================================
    
    end = EmptyOperator(task_id='end')

    # =========================================================================
    # DAG Dependencies
    # =========================================================================
    
    # Source freshness check flow
    start >> check_source_freshness_task
    check_source_freshness_task >> [source_freshness_passed, source_freshness_warning, source_freshness_failed]
    
    # Continue pipeline after successful/warning freshness check
    source_freshness_passed >> freshness_check_complete
    source_freshness_warning >> freshness_check_complete
    # source_freshness_failed does NOT connect to freshness_check_complete (blocks pipeline)
    
    # Main pipeline flow with late-arrival + schema detection
    freshness_check_complete >> dbt_deps >> dbt_debug >> staging_group
    staging_group >> intermediate_group >> check_late_arrivals_task
    check_late_arrivals_task >> [late_arrivals_detected, late_arrivals_clean]
    late_arrivals_detected >> late_arrivals_check_complete
    late_arrivals_clean >> late_arrivals_check_complete
    
    late_arrivals_check_complete >> check_schema_changes_task
    
    # Schema change branching
    check_schema_changes_task >> [schema_changed, schema_unchanged]
    schema_changed >> schema_check_complete
    schema_unchanged >> schema_check_complete
    
    # Continue to marts after checks
    schema_check_complete >> marts_group >> analytics_group
    analytics_group >> dbt_test >> dbt_docs >> end
