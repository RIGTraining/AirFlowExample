from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.models.baseoperator import chain
import pandas as pd
import logging
import os
from pathlib import Path

# Configure logging
logger = logging.getLogger(__name__)

# Default arguments for the DAG
default_args = {
    'owner': 'data_team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'email': ['admin@example.com'],
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define file paths (adjust these to your environment)
BASE_DIR = Path(__file__).parent.parent
INPUT_CSV_PATH = BASE_DIR / 'data' / 'input' / 'source_data.csv'
OUTPUT_CSV_PATH = BASE_DIR / 'data' / 'output' / 'transformed_data.csv'
TEMP_CSV_PATH = BASE_DIR / 'data' / 'temp' / 'temp_data.csv'

# Ensure directories exist
INPUT_CSV_PATH.parent.mkdir(parents=True, exist_ok=True)
OUTPUT_CSV_PATH.parent.mkdir(parents=True, exist_ok=True)
TEMP_CSV_PATH.parent.mkdir(parents=True, exist_ok=True)


def extract_csv(**context):
    """
    Extract data from source CSV file
    """
    logger.info(f"Extracting data from: {INPUT_CSV_PATH}")
    
    try:
        # Read CSV file
        df = pd.read_csv(INPUT_CSV_PATH)
        
        # Log basic information about the extracted data
        logger.info(f"Successfully extracted {len(df)} rows and {len(df.columns)} columns")
        logger.info(f"Columns: {list(df.columns)}")
        logger.info(f"Data types:\n{df.dtypes}")
        
        # Push dataframe to XCom for use in next task
        # Note: For large dataframes, consider saving to file instead
        context['ti'].xcom_push(key='extracted_data_shape', value=df.shape)
        
        # Save to temporary CSV for transformation
        df.to_csv(TEMP_CSV_PATH, index=False)
        logger.info(f"Data saved to temp file: {TEMP_CSV_PATH}")
        
        return f"Extracted {len(df)} rows"
    
    except FileNotFoundError:
        logger.error(f"Input file not found: {INPUT_CSV_PATH}")
        raise
    except Exception as e:
        logger.error(f"Error extracting data: {str(e)}")
        raise


def transform_data(**context):
    """
    Transform data using pandas with various cleaning and enrichment operations
    """
    logger.info(f"Transforming data from: {TEMP_CSV_PATH}")
    
    try:
        # Load the extracted data
        df = pd.read_csv(TEMP_CSV_PATH)
        original_count = len(df)
        logger.info(f"Loaded {original_count} rows for transformation")
        
        # ----- Transformation Examples -----
        
        # 1. Remove duplicate rows
        before_dup = len(df)
        df = df.drop_duplicates()
        after_dup = len(df)
        logger.info(f"Removed {before_dup - after_dup} duplicate rows")
        
        # 2. Handle missing values
        # Option A: Remove rows with nulls in critical columns
        critical_columns = ['id', 'date']  # Adjust based on your data
        for col in critical_columns:
            if col in df.columns:
                null_count = df[col].isnull().sum()
                if null_count > 0:
                    df = df.dropna(subset=[col])
                    logger.info(f"Dropped {null_count} rows with null in {col}")
        
        # 3. Fill missing values in other columns
        numeric_columns = df.select_dtypes(include=['number']).columns
        for col in numeric_columns:
            if df[col].isnull().any():
                df[col].fillna(df[col].median(), inplace=True)
                logger.info(f"Filled missing values in {col} with median")
        
        # 4. Convert date columns
        date_columns = [col for col in df.columns if 'date' in col.lower() or 'time' in col.lower()]
        for col in date_columns:
            try:
                df[col] = pd.to_datetime(df[col])
                logger.info(f"Converted {col} to datetime")
            except:
                logger.warning(f"Could not convert {col} to datetime")
        
        # 5. Create new derived columns (examples)
        if 'price' in df.columns and 'quantity' in df.columns:
            df['total_amount'] = df['price'] * df['quantity']
            logger.info("Created 'total_amount' column")
        
        if 'date' in df.columns:
            df['year'] = df['date'].dt.year
            df['month'] = df['date'].dt.month
            df['day_of_week'] = df['date'].dt.dayofweek
            logger.info("Created date-derived columns: year, month, day_of_week")
        
        # 6. Filter data (example: only last 90 days if date column exists)
        if 'date' in df.columns:
            recent_date = datetime.now() - timedelta(days=90)
            df = df[df['date'] >= recent_date]
            logger.info(f"Filtered to last 90 days, {len(df)} rows remaining")
        
        # 7. Sort data
        if 'date' in df.columns:
            df = df.sort_values('date', ascending=False)
        elif 'id' in df.columns:
            df = df.sort_values('id')
        
        # 8. Reset index
        df = df.reset_index(drop=True)
        
        # Log transformation results
        transformed_count = len(df)
        logger.info(f"Transformation complete: {original_count} -> {transformed_count} rows")
        logger.info(f"Data types after transformation:\n{df.dtypes}")
        
        # Save transformed data
        df.to_csv(TEMP_CSV_PATH.replace('temp', 'transformed_temp'), index=False)
        context['ti'].xcom_push(key='transformed_count', value=transformed_count)
        
        return f"Transformed {original_count} rows to {transformed_count} rows"
    
    except Exception as e:
        logger.error(f"Error transforming data: {str(e)}")
        raise


def validate_data(**context):
    """
    Validate the transformed data before loading
    """
    temp_transformed = TEMP_CSV_PATH.replace('temp', 'transformed_temp')
    logger.info(f"Validating data from: {temp_transformed}")
    
    try:
        df = pd.read_csv(temp_transformed)
        
        # Validation rules
        errors = []
        
        # Check 1: Data is not empty
        if len(df) == 0:
            errors.append("Validation failed: DataFrame is empty")
        
        # Check 2: No nulls in primary key columns
        if 'id' in df.columns and df['id'].isnull().any():
            errors.append("Validation failed: Null values found in 'id' column")
        
        # Check 3: Date column is valid if exists
        if 'date' in df.columns:
            invalid_dates = df['date'].isnull().sum()
            if invalid_dates > 0:
                errors.append(f"Validation failed: {invalid_dates} invalid dates found")
        
        # Check 4: Numeric columns have no infinite values
        numeric_cols = df.select_dtypes(include=['number']).columns
        for col in numeric_cols:
            if df[col].isin([float('inf'), float('-inf')]).any():
                errors.append(f"Validation failed: Infinite values found in {col}")
        
        # Check 5: Data quality checks
        if 'total_amount' in df.columns and (df['total_amount'] < 0).any():
            errors.append("Validation failed: Negative total_amount values found")
        
        if errors:
            for error in errors:
                logger.error(error)
            raise ValueError("Data validation failed: " + "; ".join(errors))
        
        logger.info(f"Validation successful: {len(df)} rows passed all checks")
        context['ti'].xcom_push(key='validated_count', value=len(df))
        
        return "Data validation successful"
    
    except Exception as e:
        logger.error(f"Validation error: {str(e)}")
        raise


def load_csv(**context):
    """
    Load transformed data to output CSV file
    """
    temp_transformed = TEMP_CSV_PATH.replace('temp', 'transformed_temp')
    logger.info(f"Loading data from: {temp_transformed} to {OUTPUT_CSV_PATH}")
    
    try:
        # Read transformed data
        df = pd.read_csv(temp_transformed)
        
        # Create a timestamped version for audit trail
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        timestamped_output = OUTPUT_CSV_PATH.parent / f"transformed_data_{timestamp}.csv"
        
        # Save to main output file
        df.to_csv(OUTPUT_CSV_PATH, index=False)
        logger.info(f"Data saved to: {OUTPUT_CSV_PATH}")
        
        # Save timestamped version for historical tracking
        df.to_csv(timestamped_output, index=False)
        logger.info(f"Archived version saved to: {timestamped_output}")
        
        # Log summary statistics
        logger.info(f"Successfully loaded {len(df)} rows to CSV")
        logger.info(f"File size: {OUTPUT_CSV_PATH.stat().st_size / 1024:.2f} KB")
        
        # Push metadata to XCom
        context['ti'].xcom_push(key='output_path', value=str(OUTPUT_CSV_PATH))
        context['ti'].xcom_push(key='row_count', value=len(df))
        context['ti'].xcom_push(key='column_count', value=len(df.columns))
        
        # Clean up temporary files
        os.remove(temp_transformed)
        if TEMP_CSV_PATH.exists():
            os.remove(TEMP_CSV_PATH)
        logger.info("Temporary files cleaned up")
        
        return f"Successfully loaded {len(df)} rows to {OUTPUT_CSV_PATH}"
    
    except Exception as e:
        logger.error(f"Error loading data: {str(e)}")
        raise


def generate_report(**context):
    """
    Generate a summary report of the ETL process
    """
    ti = context['ti']
    
    # Pull data from XCom
    extracted_shape = ti.xcom_pull(key='extracted_data_shape', task_ids='extract_task')
    transformed_count = ti.xcom_pull(key='transformed_count', task_ids='transform_task')
    validated_count = ti.xcom_pull(key='validated_count', task_ids='validate_task')
    output_path = ti.xcom_pull(key='output_path', task_ids='load_task')
    row_count = ti.xcom_pull(key='row_count', task_ids='load_task')
    column_count = ti.xcom_pull(key='column_count', task_ids='load_task')
    
    # Generate report
    report = f"""
    ====================================
    CSV ETL Pipeline Report
    ====================================
    Execution Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
    DAG Run ID: {context['dag_run'].run_id if context.get('dag_run') else 'N/A'}
    
    Extraction:
    - Source file: {INPUT_CSV_PATH}
    - Rows extracted: {extracted_shape[0] if extracted_shape else 'N/A'}
    - Columns extracted: {extracted_shape[1] if extracted_shape else 'N/A'}
    
    Transformation:
    - Rows after transformation: {transformed_count if transformed_count else 'N/A'}
    
    Validation:
    - Rows validated: {validated_count if validated_count else 'N/A'}
    
    Loading:
    - Output file: {output_path if output_path else 'N/A'}
    - Rows loaded: {row_count if row_count else 'N/A'}
    - Columns loaded: {column_count if column_count else 'N/A'}
    
    Success Rate: {((validated_count or 0) / (extracted_shape[0] or 1) * 100):.2f}%
    ====================================
    """
    
    logger.info(report)
    
    # Save report to file
    report_path = BASE_DIR / 'data' / 'logs' / f"etl_report_{datetime.now().strftime('%Y%m%d')}.txt"
    report_path.parent.mkdir(parents=True, exist_ok=True)
    
    with open(report_path, 'a') as f:
        f.write(report)
        f.write("\n")
    
    return "Report generated successfully"


# Define the DAG
dag = DAG(
    'csv_etl_pipeline',
    default_args=default_args,
    description='Extract CSV, transform with pandas, load to CSV',
    schedule_interval='@daily',  # Run daily at midnight
    catchup=False,
    tags=['csv', 'etl', 'pandas'],
    max_active_runs=1,  # Prevent concurrent runs
)

# Define tasks
start_task = EmptyOperator(
    task_id='start',
    dag=dag,
)

extract_task = PythonOperator(
    task_id='extract_csv',
    python_callable=extract_csv,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    dag=dag,
)

validate_task = PythonOperator(
    task_id='validate_data',
    python_callable=validate_data,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_csv',
    python_callable=load_csv,
    dag=dag,
)

report_task = PythonOperator(
    task_id='generate_report',
    python_callable=generate_report,
    dag=dag,
)

end_task = EmptyOperator(
    task_id='end',
    dag=dag,
)

# Set task dependencies
chain(
    start_task,
    extract_task,
    transform_task,
    validate_task,
    load_task,
    report_task,
    end_task
)