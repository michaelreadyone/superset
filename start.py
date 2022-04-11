import os

from superset import create_app

if __name__ == '__main__':
    superset_app = create_app()
    superset_app.run(host='0.0.0.0', port='8088', debug=True)