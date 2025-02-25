from google.cloud import storage
from google.cloud import aiplatform

import datetime
import json

def download_blob(bucket_name, source_blob_name, destination_file_name):
    """Downloads a blob from the bucket."""

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)

    blob = bucket.blob(source_blob_name)
    # convert to string
    data = blob.download_as_string().decode()
    return json.loads(data)

def trigger_pipeline(event, context):
    """Triggered by a change to a Cloud Storage bucket.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    print(f"Processing file: {event['name']}.")
    
    print(f"downloading config.json")
    config_file = "config.json"
    config = download_blob(bucket_name = "qwiklabs-gcp-01-3aa41e6df6c3-bucket"
              , source_blob_name = "config.json"
              , destination_file_name = config_file)
    
    print(f"existing config : {config}")
    #overwrite the `input_data_filename` in configuration
    config["input_data_filename"] = event['name']
    print(f"Final config : {config}")
    
    #download pipeline definition
    print(f"downloading pipeline template")
    pipeline_template = config.get("pipeline_package_path")
    pipeline_template_json = download_blob(bucket_name = "qwiklabs-gcp-01-3aa41e6df6c3-bucket"
        , source_blob_name = "tabular-data-regression-kfp-cicd-pipeline.json"
        , destination_file_name = pipeline_template)
    
    print(f"downloading pipeline template complete")
    PIPELINE_ROOT = f"{config.get('staging_bucket_uri')}/pipeline_root/"
    print(f"PIPELINE_ROOT : {PIPELINE_ROOT}")
    
    pipeline_package_path = "/tmp/" + pipeline_template
    pipeline_display_name = config.get('pipeline_name')

    with open(pipeline_package_path, 'w', encoding='utf-8') as f:
        json.dump(pipeline_template_json, f, ensure_ascii=False, indent=4)
    
    print(f"submit pipeline job")
    job = aiplatform.PipelineJob(
        display_name=pipeline_display_name,
        template_path=pipeline_package_path,
        pipeline_root=PIPELINE_ROOT,
        parameter_values=config,
    )
    
    SERVICE_ACCOUNT = config.get("service_account")
    job.submit(service_account = SERVICE_ACCOUNT)
