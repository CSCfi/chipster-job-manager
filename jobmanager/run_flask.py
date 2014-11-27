import yaml
import argparse

from models import Base
from utils import config_to_db_session
from www import app

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('config_file')
    args = parser.parse_args()

    config = yaml.load(open(args.config_file))
    app.config['params'] = config
    app.config['DB'] = config_to_db_session(config, Base)
    app.run(debug=config.get('debug'))

if __name__ == '__main__':
    main()
