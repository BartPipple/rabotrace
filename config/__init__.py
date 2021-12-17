from pyaml import yaml
import os


config_filename = os.path.join(os.path.dirname(__file__), 'config_develop.yml')


def read_config(path):
    with open(path, 'rt') as f:
        return yaml.safe_load(f.read())


config = read_config(config_filename)


def get_db_user():
    return config['database']['user']


def get_db_password():
    return config['database']['password']


def get_db_name():
    return config['database']['name']


def get_db_url():
    return config['database']['url']

