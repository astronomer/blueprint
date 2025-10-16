from datetime import datetime, timezone
from typing import List

from blueprint import BaseModel, Blueprint, Field


class ComplexDependenciesConfig(BaseModel):
    """Configuration for demonstrating complex task dependencies."""

    job_id: str = Field(pattern=r"^[a-zA-Z0-9_-]+$", description="Unique identifier for this job")
    data_sources: List[str] = Field(
        default=["orders", "customers", "products"],
        description="List of data sources to process"
    )
    enable_data_quality: bool = Field(
        default=True, 
        description="Whether to run data quality checks"
    )
    enable_notifications: bool = Field(
        default=False, 
        description="Whether to send notifications on completion"
    )
    parallel_extract: bool = Field(
        default=True, 
        description="Whether to extract sources in parallel"
    )
    schedule: str = Field(default="@daily", description="Cron expression or Airflow preset")


class ComplexDependencies(Blueprint[ComplexDependenciesConfig]):
    """Demonstrates complex task dependency patterns including branching, joining, and conditional execution."""

    def render(self, config: ComplexDependenciesConfig):
        from airflow import DAG
        from airflow.decorators import task
        from airflow.operators.bash import BashOperator
        from airflow.operators.dummy import DummyOperator
        from airflow.operators.python import PythonOperator, BranchPythonOperator
        from airflow.utils.trigger_rule import TriggerRule

        dag = DAG(
            dag_id=config.job_id,
            description="Complex dependency patterns with branching and conditional execution",
            schedule=config.schedule,
            start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
            catchup=False,
            tags=["dependencies", "blueprint", "complex"],
        )

        with dag:
            # Start task
            start = DummyOperator(task_id="start")

            # Configuration check - determines pipeline flow
            def check_configuration(**context):
                """Check configuration and decide pipeline flow."""
                config_check_results = {
                    "data_sources_count": len(config.data_sources),
                    "quality_enabled": config.enable_data_quality,
                    "notifications_enabled": config.enable_notifications,
                    "parallel_extract": config.parallel_extract
                }
                
                print(f"Configuration check: {config_check_results}")
                
                # Return next task based on configuration
                if len(config.data_sources) > 2:
                    return "setup_parallel_processing"
                else:
                    return "setup_sequential_processing"

            config_check = BranchPythonOperator(
                task_id="config_check",
                python_callable=check_configuration
            )

            # Setup tasks (branching based on configuration)
            setup_parallel = DummyOperator(task_id="setup_parallel_processing")
            setup_sequential = DummyOperator(task_id="setup_sequential_processing")

            # Data extraction tasks (dynamic based on sources)
            extract_tasks = []
            for source in config.data_sources:
                @task(task_id=f"extract_{source}")
                def extract_source(source_name: str = source):
                    print(f"Extracting data from {source_name}")
                    # Simulate extraction with different processing times
                    import time
                    time.sleep(1)  # Simulate processing
                    return {
                        "source": source_name,
                        "records": 1000 + hash(source_name) % 1000,
                        "status": "success"
                    }
                
                extract_task = extract_source()
                extract_tasks.append(extract_task)

            # Data validation tasks (one per source)
            validate_tasks = []
            for i, source in enumerate(config.data_sources):
                def create_validate_task(source_name, task_index):
                    def validate_data(**context):
                        # Get data from corresponding extract task
                        ti = context['ti']
                        extracted_data = ti.xcom_pull(task_ids=f"extract_{source_name}")
                        
                        print(f"Validating data from {source_name}")
                        
                        # Simulate validation logic
                        if extracted_data and extracted_data.get("records", 0) > 0:
                            print(f"Validation passed for {source_name}: {extracted_data['records']} records")
                            return {"source": source_name, "validation": "passed", "records": extracted_data["records"]}
                        else:
                            print(f"Validation failed for {source_name}")
                            return {"source": source_name, "validation": "failed", "records": 0}
                    
                    return PythonOperator(
                        task_id=f"validate_{source_name}",
                        python_callable=validate_data
                    )
                
                validate_task = create_validate_task(source, i)
                validate_tasks.append(validate_task)

            # Join point after all extractions
            extraction_complete = DummyOperator(
                task_id="extraction_complete",
                trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS
            )

            # Data quality assessment (conditional)
            def assess_data_quality(**context):
                """Assess overall data quality from all sources."""
                ti = context['ti']
                
                validation_results = []
                for source in config.data_sources:
                    result = ti.xcom_pull(task_ids=f"validate_{source}")
                    validation_results.append(result)
                
                total_records = sum(r.get("records", 0) for r in validation_results if r)
                passed_validations = sum(1 for r in validation_results if r and r.get("validation") == "passed")
                
                quality_score = (passed_validations / len(config.data_sources)) * 100 if config.data_sources else 0
                
                quality_report = {
                    "total_sources": len(config.data_sources),
                    "passed_validations": passed_validations,
                    "total_records": total_records,
                    "quality_score": quality_score
                }
                
                print(f"Data quality assessment: {quality_report}")
                
                # Decide next step based on quality
                if quality_score >= 80:
                    return "data_transformation"
                else:
                    return "data_quality_remediation"

            quality_assessment = BranchPythonOperator(
                task_id="data_quality_assessment",
                python_callable=assess_data_quality
            )

            # Data transformation (main processing)
            @task
            def transform_data():
                """Transform and combine data from all sources."""
                print("Transforming and combining data from all sources")
                
                # Simulate complex transformation
                transformed_data = {
                    "transformation_type": "join_and_aggregate",
                    "output_records": sum(1000 + hash(source) % 1000 for source in config.data_sources),
                    "transformations_applied": ["join", "deduplicate", "aggregate", "calculate_metrics"]
                }
                
                print(f"Transformation complete: {transformed_data}")
                return transformed_data

            data_transformation = transform_data()

            # Data quality remediation (alternative path)
            remediation = BashOperator(
                task_id="data_quality_remediation",
                bash_command='echo "Running data quality remediation procedures..."'
            )

            # Join point after transformation or remediation
            processing_complete = DummyOperator(
                task_id="processing_complete",
                trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS
            )

            # Final output tasks (parallel)
            @task
            def generate_reports():
                """Generate final reports."""
                print("Generating comprehensive reports")
                return {"reports": ["summary_report", "quality_report", "metrics_report"]}

            @task
            def update_metadata():
                """Update system metadata."""
                print("Updating metadata and lineage information")
                return {"metadata_updated": True, "lineage_recorded": True}

            generate_reports_task = generate_reports()
            update_metadata_task = update_metadata()

            # Conditional notification
            def check_notification_needed(**context):
                """Check if notifications should be sent."""
                if config.enable_notifications:
                    return "send_notifications"
                else:
                    return "skip_notifications"

            notification_check = BranchPythonOperator(
                task_id="notification_check",
                python_callable=check_notification_needed
            )

            send_notifications = BashOperator(
                task_id="send_notifications",
                bash_command='echo "Sending completion notifications..."'
            )

            skip_notifications = DummyOperator(task_id="skip_notifications")

            # Final completion
            complete = DummyOperator(
                task_id="pipeline_complete",
                trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS
            )

            # Define complex dependency relationships
            
            # Initial flow
            start >> config_check
            config_check >> [setup_parallel, setup_sequential]
            
            # Extraction dependencies based on configuration
            if config.parallel_extract:
                # Parallel extraction
                [setup_parallel, setup_sequential] >> extract_tasks
            else:
                # Sequential extraction
                [setup_parallel, setup_sequential] >> extract_tasks[0]
                for i in range(len(extract_tasks) - 1):
                    extract_tasks[i] >> extract_tasks[i + 1]
            
            # Validation dependencies (each validate depends on corresponding extract)
            for extract_task, validate_task in zip(extract_tasks, validate_tasks):
                extract_task >> validate_task
            
            # Join after extraction and validation
            validate_tasks >> extraction_complete
            
            # Quality assessment (conditional)
            if config.enable_data_quality:
                extraction_complete >> quality_assessment
                quality_assessment >> [data_transformation, remediation]
                [data_transformation, remediation] >> processing_complete
            else:
                extraction_complete >> data_transformation >> processing_complete
            
            # Final parallel tasks
            processing_complete >> [generate_reports_task, update_metadata_task]
            [generate_reports_task, update_metadata_task] >> notification_check
            
            # Notification flow
            notification_check >> [send_notifications, skip_notifications]
            [send_notifications, skip_notifications] >> complete

        return dag