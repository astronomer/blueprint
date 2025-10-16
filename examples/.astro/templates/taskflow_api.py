from datetime import datetime, timezone
from typing import Any, Dict, List

from blueprint import BaseModel, Blueprint, Field


class TaskFlowAPIConfig(BaseModel):
    """Configuration for TaskFlow API demonstration."""

    job_id: str = Field(pattern=r"^[a-zA-Z0-9_-]+$", description="Unique identifier for this job")
    data_source: str = Field(description="Source system to extract data from")
    processing_steps: List[str] = Field(
        default=["validate", "clean", "transform", "enrich"],
        description="List of processing steps to apply"
    )
    output_format: str = Field(
        default="parquet", 
        description="Output format for processed data",
        pattern=r"^(parquet|csv|json)$"
    )
    parallel_processing: bool = Field(
        default=True, 
        description="Whether to enable parallel processing for independent steps"
    )
    schedule: str = Field(default="@daily", description="Cron expression or Airflow preset")


class TaskFlowAPI(Blueprint[TaskFlowAPIConfig]):
    """Demonstrates TaskFlow API usage with XCom passing and dynamic task creation."""

    def render(self, config: TaskFlowAPIConfig):
        from airflow import DAG
        from airflow.decorators import dag, task
        from airflow.operators.python import get_current_context

        @dag(
            dag_id=config.job_id,
            description=f"TaskFlow API demo processing {config.data_source}",
            schedule=config.schedule,
            start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
            catchup=False,
            tags=["taskflow", "blueprint", "demo"],
        )
        def taskflow_demo():
            @task
            def extract_data() -> Dict[str, Any]:
                """Extract data from source using TaskFlow API."""
                print(f"Extracting data from {config.data_source}")
                
                # Simulate data extraction
                extracted_data = {
                    "source": config.data_source,
                    "records": [
                        {"id": 1, "name": "Alice", "value": 100},
                        {"id": 2, "name": "Bob", "value": 200},
                        {"id": 3, "name": "Charlie", "value": 300},
                    ],
                    "metadata": {
                        "extraction_time": datetime.now().isoformat(),
                        "record_count": 3
                    }
                }
                
                print(f"Extracted {len(extracted_data['records'])} records")
                return extracted_data

            @task
            def validate_data(data: Dict[str, Any]) -> Dict[str, Any]:
                """Validate extracted data quality."""
                print("Validating data quality...")
                
                valid_records = []
                invalid_count = 0
                
                for record in data["records"]:
                    if record.get("id") and record.get("name") and record.get("value", 0) > 0:
                        valid_records.append(record)
                    else:
                        invalid_count += 1
                
                validated_data = {
                    **data,
                    "records": valid_records,
                    "validation": {
                        "valid_count": len(valid_records),
                        "invalid_count": invalid_count,
                        "validation_passed": invalid_count == 0
                    }
                }
                
                print(f"Validation complete: {len(valid_records)} valid, {invalid_count} invalid")
                return validated_data

            @task
            def clean_data(data: Dict[str, Any]) -> Dict[str, Any]:
                """Clean and standardize data."""
                print("Cleaning data...")
                
                cleaned_records = []
                for record in data["records"]:
                    cleaned_record = {
                        "id": record["id"],
                        "name": record["name"].strip().title(),
                        "value": float(record["value"])
                    }
                    cleaned_records.append(cleaned_record)
                
                cleaned_data = {
                    **data,
                    "records": cleaned_records,
                    "cleaning": {
                        "cleaned_count": len(cleaned_records),
                        "cleaning_applied": ["strip", "title_case", "float_conversion"]
                    }
                }
                
                print(f"Cleaned {len(cleaned_records)} records")
                return cleaned_data

            @task
            def transform_data(data: Dict[str, Any]) -> Dict[str, Any]:
                """Apply business transformations."""
                print("Applying transformations...")
                
                transformed_records = []
                for record in data["records"]:
                    transformed_record = {
                        **record,
                        "value_squared": record["value"] ** 2,
                        "category": "high" if record["value"] > 150 else "low",
                        "processed_at": datetime.now().isoformat()
                    }
                    transformed_records.append(transformed_record)
                
                transformed_data = {
                    **data,
                    "records": transformed_records,
                    "transformation": {
                        "transformed_count": len(transformed_records),
                        "transformations_applied": ["value_squared", "category", "processed_at"]
                    }
                }
                
                print(f"Transformed {len(transformed_records)} records")
                return transformed_data

            @task
            def enrich_data(data: Dict[str, Any]) -> Dict[str, Any]:
                """Enrich data with additional information."""
                print("Enriching data...")
                
                # Simulate external data lookup
                enrichment_data = {
                    1: {"region": "North", "tier": "Premium"},
                    2: {"region": "South", "tier": "Standard"},
                    3: {"region": "East", "tier": "Premium"}
                }
                
                enriched_records = []
                for record in data["records"]:
                    enrichment = enrichment_data.get(record["id"], {"region": "Unknown", "tier": "Basic"})
                    enriched_record = {
                        **record,
                        **enrichment
                    }
                    enriched_records.append(enriched_record)
                
                enriched_data = {
                    **data,
                    "records": enriched_records,
                    "enrichment": {
                        "enriched_count": len(enriched_records),
                        "enrichment_fields": ["region", "tier"]
                    }
                }
                
                print(f"Enriched {len(enriched_records)} records")
                return enriched_data

            @task
            def generate_summary(data: Dict[str, Any]) -> Dict[str, Any]:
                """Generate processing summary."""
                print("Generating processing summary...")
                
                records = data["records"]
                total_value = sum(record["value"] for record in records)
                avg_value = total_value / len(records) if records else 0
                
                summary = {
                    "total_records": len(records),
                    "total_value": total_value,
                    "average_value": avg_value,
                    "categories": {
                        "high": len([r for r in records if r.get("category") == "high"]),
                        "low": len([r for r in records if r.get("category") == "low"])
                    },
                    "regions": list(set(r.get("region", "Unknown") for r in records))
                }
                
                print(f"Summary: {summary}")
                return summary

            @task
            def save_data(data: Dict[str, Any], summary: Dict[str, Any]) -> str:
                """Save processed data in specified format."""
                print(f"Saving data in {config.output_format} format...")
                
                output_path = f"/tmp/{config.job_id}_output.{config.output_format}"
                
                # Simulate saving data
                if config.output_format == "json":
                    print(f"Saving {len(data['records'])} records as JSON to {output_path}")
                elif config.output_format == "csv":
                    print(f"Saving {len(data['records'])} records as CSV to {output_path}")
                else:  # parquet
                    print(f"Saving {len(data['records'])} records as Parquet to {output_path}")
                
                print(f"Summary saved: {summary}")
                return output_path

            # Create dynamic processing pipeline
            extracted_data = extract_data()
            
            # Sequential processing steps based on configuration
            current_data = extracted_data
            for step in config.processing_steps:
                if step == "validate":
                    current_data = validate_data(current_data)
                elif step == "clean":
                    current_data = clean_data(current_data)
                elif step == "transform":
                    current_data = transform_data(current_data)
                elif step == "enrich":
                    current_data = enrich_data(current_data)
            
            # Generate summary and save (can run in parallel if enabled)
            if config.parallel_processing:
                # Summary and save can run in parallel after processing
                summary_result = generate_summary(current_data)
                save_result = save_data(current_data, summary_result)
            else:
                # Sequential execution
                summary_result = generate_summary(current_data)
                save_result = save_data(current_data, summary_result)

        return taskflow_demo()