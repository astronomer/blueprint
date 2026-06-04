"""Build DAGs programmatically with the Builder API instead of YAML.

Generates one telemetry DAG per satellite, reusing the blueprints in
``blueprints.py``. ``DAGConfig`` takes the same fields you would write in YAML.
"""

from blueprint import Builder, DAGConfig

SATELLITES = [
    {"id": "SAT-001", "target": "Kepler-22b", "ground_station": "Madrid"},
    {"id": "SAT-002", "target": "Proxima-b", "ground_station": "Goldstone"},
    {"id": "SAT-003", "target": "TRAPPIST-1e", "ground_station": "Canberra"},
]

builder = Builder()

for satellite in SATELLITES:
    slug = satellite["id"].lower().replace("-", "_")

    config = DAGConfig(
        dag_id=f"telemetry_{slug}",
        schedule="@hourly",
        description=f"Telemetry pipeline for {satellite['id']}",
        priority="high",
        steps={
            "scan": {
                "blueprint": "scan",
                "version": 1,
                "target": satellite["target"],
            },
            "analyze": {
                "blueprint": "analyze",
                "depends_on": ["scan"],
                "algorithms": ["fft", "matched_filter"],
            },
            "transmit": {
                "blueprint": "transmit",
                "depends_on": ["analyze"],
                "destination": satellite["ground_station"],
            },
        },
    )

    dag = builder.build(config)
    globals()[dag.dag_id] = dag
