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
