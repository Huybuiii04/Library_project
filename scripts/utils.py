"""
Utility functions for the project
"""

def get_mysql_config():
    """Get MySQL connection configuration"""
    return {
        'user': 'admin',
        'password': 'admin',
        'host': 'localhost',
        'database': 'project_db',
        'port': 3306,
        'allow_local_infile': True
    }

def check_minio_has_data(bucket, prefix) -> bool:
    client = Minio(
        "minio:9000",
        access_key="minioadmin",
        secret_key="minioadmin",
        secure=False
    )

    return any(client.list_objects(bucket, prefix=prefix, recursive=True))