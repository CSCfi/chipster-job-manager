from flask import Flask
from functools import wraps

app = Flask(__name__)

def with_db_session(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        return f(app.config['DB'](), *args, **kwargs)
    return wrapper

import www.httpserver
