#!/usr/bin/env python
"""
Generate a secure Fernet key for Apache Airflow.
Run this script to generate a key, then add it to your .env file.
"""

from cryptography.fernet import Fernet

if __name__ == "__main__":
    # Generate a Fernet key
    fernet_key = Fernet.generate_key().decode()
    
    print("\nGenerated Fernet key for Airflow:")
    print(f"{fernet_key}")
    print("\nAdd this to your .env file as:")
    print(f"AIRFLOW_FERNET_KEY={fernet_key}")
    print("\nNEVER commit this key to version control!") 