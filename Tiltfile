docker_compose('examples/docker-compose.yaml')

sync_pyproj_toml = sync('./pyproject.toml', '/usr/local/airflow/blueprint/pyproject.toml')
sync_readme = sync('./README.md', '/usr/local/airflow/blueprint/README.md')
sync_src = sync('./blueprint', '/usr/local/airflow/blueprint/blueprint')

docker_build(
    'blueprint-airflow',
    context='.',
    dockerfile='examples/Dockerfile',
    ignore=['.venv', '**/logs/**'],
    live_update=[
        sync_pyproj_toml,
        sync_src,
        sync_readme,
        run(
            'cd /usr/local/airflow/blueprint && uv pip install -e .',
            trigger=['pyproject.toml']
        ),
    ]
)
