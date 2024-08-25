import os
from dotenv import load_dotenv

# This function will load environment variables from the .env file
def load_env_vars() -> dict:
 # Load environment variables from .env file
    load_dotenv()

    # Return the variables as a dictionary
    return {
        'DB_NAME': os.getenv('DB_NAME'),
        'DB_USER': os.getenv('DB_USER'),
        'DB_PASSWORD': os.getenv('DB_PASSWORD'),
        'DB_HOST': os.getenv('DB_HOST'),
        'DB_PORT': os.getenv('DB_PORT'),
        'OPENAI_API_KEY': os.getenv('OPENAI_API_KEY')
    }
