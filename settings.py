import json

from sqlalchemy import create_engine


config_json = None
with open('config.json') as json_file:  
    config_json = json.load(json_file)


DB_HOST = config_json['DB']['HOST']
DB_NAME = config_json['DB']['DB_NAME']
DB_USER = config_json['DB']['USER']
DB_PASSWORD = config_json['DB']['PASSWORD']
DB_PORT = config_json['DB']['PORT']

CHUNK_SIZE = config_json.get('CHUNK_SIZE', 30)  # To prevent overhauls
CHUNK_SLEEP_TIME = config_json.get('CHUNK_SLEEP_TIME', 4)  # in seconds

engine = create_engine(
    'mysql+mysqlconnector://{user}:{password}@{host}:{port}/{dbname}'.format(
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
    ),
    pool_recycle=3600,
    echo=False,  # TODO: Provide command with --verbose switch, so user can see logs if he wants to.
)

