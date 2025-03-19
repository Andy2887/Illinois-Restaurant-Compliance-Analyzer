import boto3
import argparse
import sys
import pandas as pd
import tempfile
import os
from tabulate import tabulate

class EMRClient:
    def __init__(self, region='us-east-2'):
        self.emr_client = boto3.client('emr', region_name=region)
        self.s3_client = boto3.client('s3', region_name=region)

    def add_spark_step(self, cluster_id, script_path, data_source, output_uri):
        """
        Add a Spark step to an existing EMR cluster
        
        Parameters:
        -----------
        cluster_id : str
            The ID of the EMR cluster (e.g., j-1VE06SA9NF1LV)
        script_path : str
            S3 path to the PySpark script
        data_source : str
            S3 path to the input data
        output_uri : str
            S3 path for the output data
        
        Returns:
        --------
        dict
            Response from EMR API containing step IDs
        """
        step_config = {
            'Name': 'ProcessViolationsData',
            'ActionOnFailure': 'CONTINUE',
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': [
                    'spark-submit',
                    script_path,
                    '--data_source', data_source,
                    '--output_uri', output_uri
                ]
            }
        }
        
        print(f"Adding Spark step to cluster {cluster_id}")
        print(f"  Script: {script_path}")
        print(f"  Input: {data_source}")
        print(f"  Output: {output_uri}")
        
        response = self.emr_client.add_job_flow_steps(
            JobFlowId=cluster_id,
            Steps=[step_config]
        )
        
        return response
    
    def describe_step(self, cluster_id, step_id):
        """Get the status of a step"""
        response = self.emr_client.describe_step(
            ClusterId=cluster_id,
            StepId=step_id
        )
        return response['Step']
    
    def wait_for_step_completion(self, cluster_id, step_id, poll_interval=30):
        """Wait for a step to complete and print status updates"""
        import time
        
        print(f"Waiting for step {step_id} to complete...")
        while True:
            step_info = self.describe_step(cluster_id, step_id)
            status = step_info['Status']['State']
            
            print(f"Current status: {status}")
            
            if status in ['COMPLETED', 'FAILED', 'CANCELLED']:
                print(f"Step finished with status: {status}")
                return status == 'COMPLETED'
            
            time.sleep(poll_interval)

    def read_parquet_results(self, output_uri, limit=10):
        """
        Read and display results from parquet files in S3
        
        Parameters:
        -----------
        output_uri : str
            S3 URI where the parquet files are stored (e.g., s3://bucket/path/)
        limit : int
            Maximum number of rows to display
        """
        try:
            # Parse the S3 URI
            if not output_uri.startswith('s3://'):
                raise ValueError("Output URI must start with 's3://'")
            
            s3_parts = output_uri.replace('s3://', '').split('/', 1)
            if len(s3_parts) < 2:
                raise ValueError("Invalid S3 URI format. Should be 's3://bucket/path/'")
            
            bucket = s3_parts[0]
            prefix = s3_parts[1]
            if not prefix.endswith('/'):
                prefix += '/'
            
            # List objects in the S3 location
            print(f"Listing objects in {output_uri}...")
            response = self.s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)

            if 'Contents' not in response:
                print(f"No objects found in {output_uri}")
                return
            
            # Find parquet files (look for part-* files or _SUCCESS file to confirm completion)
            parquet_files = []
            has_success = False
            
            for obj in response['Contents']:
                key = obj['Key']
                if key.endswith('_SUCCESS'):
                    has_success = True
                elif key.endswith('.parquet') or '/part-' in key:
                    parquet_files.append(key)
            
            if not parquet_files:
                print(f"No parquet files found in {output_uri}")
                if has_success:
                    print("Found _SUCCESS marker but no data files. The query may have returned no results.")
                return
            

            # Create a temporary directory to store the parquet files
            with tempfile.TemporaryDirectory() as temp_dir:
                all_data = []
                
                # Download and read each parquet file
                for i, key in enumerate(parquet_files):
                    local_path = os.path.join(temp_dir, f"part-{i}.parquet")
                    print(f"Downloading {key} to temporary location...")
                    self.s3_client.download_file(bucket, key, local_path)
                    
                    # Read the parquet file into a pandas DataFrame
                    df_part = pd.read_parquet(local_path)
                    all_data.append(df_part)
                
                # Combine all the parts
                if all_data:
                    df = pd.concat(all_data, ignore_index=True)
                    
                    # Limit the number of rows to display
                    if limit > 0:
                        df = df.head(limit)
                    
                    # Display the data
                    print("\nRESTAURANT VIOLATIONS ANALYSIS RESULTS:")
                    print("="*50)
                    print(tabulate(df, headers='keys', tablefmt='psql', showindex=False))
                    print("="*50)
                    print(f"Showing {len(df)} of {sum(len(data) for data in all_data)} rows")
                    print(f"Total files in output: {len(parquet_files)}")
                    
                    return df
                else:
                    print("No data could be read from the parquet files.")
                    return None

        except Exception as e:
            print(f"Error reading parquet results: {str(e)}")
            import traceback
            traceback.print_exc()
            return None
    

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='EMR Client for running Spark jobs')
    parser.add_argument('--cluster-id', required=True, help='EMR Cluster ID')
    parser.add_argument('--region', default='us-east-2', help='AWS region')
    parser.add_argument('--script', required=True, help='S3 path to PySpark script')
    parser.add_argument('--input', required=True, help='S3 path to input data')
    parser.add_argument('--output', required=True, help='S3 path for output data')
    parser.add_argument('--wait', action='store_true', help='Wait for job completion')
    parser.add_argument('--show-results', action='store_true', help='Show results after job completion')
    parser.add_argument('--limit', type=int, default=10, help='Maximum number of result rows to display')
    
    args = parser.parse_args()
    
    client = EMRClient(region=args.region)
    
    # Add the step to the EMR cluster
    response = client.add_spark_step(
        cluster_id=args.cluster_id,
        script_path=args.script,
        data_source=args.input,
        output_uri=args.output
    )
    
    step_id = response['StepIds'][0]
    print(f"Successfully added step with ID: {step_id}")
    
    # Wait for completion if requested
    if args.wait:
        success = client.wait_for_step_completion(args.cluster_id, step_id)
        if not success:
            print("Step failed to complete successfully.")
            sys.exit(1)
        
        # Show results if requested
        if args.show_results:
            client.read_parquet_results(
                args.output, 
                limit=args.limit
            )