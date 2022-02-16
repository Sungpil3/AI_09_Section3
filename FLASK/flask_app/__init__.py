from flask import Flask

def create_app():

    app = Flask(__name__)
    from flask_app.routes import main_routes
    from flask_app.routes import result_routes
    app.register_blueprint(main_routes.bp)
    app.register_blueprint(result_routes.bp)
    return app

if __name__ == "__main__":
  app = create_app()
  app.run()    