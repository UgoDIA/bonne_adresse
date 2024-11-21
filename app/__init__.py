from flask import Flask
import os
try:
    import dotenv
except ImportError:
    print("Le package dotenv n'est pas installé.")
else:
    dotenv.load_dotenv()

def create_app():
    app = Flask(__name__, template_folder='../templates',static_folder='../static')

    app.secret_key = os.environ['SECRET_KEY']
    
    from .main.routes import main_bp
    from .api.routes import api_bp
    
    app.register_blueprint(main_bp)
    app.register_blueprint(api_bp)

    
    return app

