from callreview.web import create_app
from callreview.config import settings

app = create_app()

if __name__ == "__main__":
    app.run(host=settings.web_host, port=settings.web_port, debug=True)