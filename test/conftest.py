import pytest
import time
import testcontainers.core.container
from src.app import app as sample_app

@pytest.fixture
def app():
    yield sample_app


@pytest.fixture
def client(app):
    return app.test_client()

@pytest.fixture
def kafka():
    with testcontainers.core.container.DockerContainer("spotify/kafka") as kafka:
        time.sleep(20)