import pytest


@pytest.fixture(autouse=True)
def env_setup(monkeypatch):
    monkeypatch.setenv("MEDIAHAVEN_USERNAME", "username")
    monkeypatch.setenv("MEDIAHAVEN_PASSWORD", "password")
    monkeypatch.setenv("MEDIAHAVEN_HOST", "host")
    monkeypatch.setenv("RABBITMQ_USERNAME", "username")
    monkeypatch.setenv("RABBITMQ_PASSWORD", "password")
    monkeypatch.setenv("RABBITMQ_HOST", "host")
    monkeypatch.setenv("RABBITMQ_QUEUE", "queue")
    monkeypatch.setenv("RABBITMQ_EXCHANGE", "exchange")
    monkeypatch.setenv("RABBITMQ_ESSENCE_LINKED_ROUTING_KEY", "essence_linked")
    monkeypatch.setenv("RABBITMQ_ESSENCE_UNLINKED_ROUTING_KEY", "essence_unlinked")
    monkeypatch.setenv("RABBITMQ_OBJECT_DELETED_ROUTING_KEY", "object_deleted")
    monkeypatch.setenv("RABBITMQ_GET_METADATA_ROUTING_KEY", "get_metadata")
