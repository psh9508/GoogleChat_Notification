import json
import os

import pytest

os.environ.setdefault("SECRET_ARN", "arn:aws:secretsmanager:us-east-1:123456789012:secret:test")
os.environ.setdefault("DYNAMODB_TABLE_NAME", "test-table")

import notification_function.app as handler


class DummyTable:
    def __init__(self):
        self.items = {}

    def get_item(self, Key, ConsistentRead=False):
        item = self.items.get(Key["PK"])
        return {"Item": item} if item else {}

    def put_item(self, Item):
        self.items[Item["PK"]] = Item


@pytest.fixture(autouse=True)
def patch_deps(monkeypatch):
    table = DummyTable()
    sent = []
    monkeypatch.setattr(handler, "table", table)
    monkeypatch.setattr(handler, "get_all_webhooks", lambda: {"team": "https://chat.example/hook"})
    monkeypatch.setattr(handler, "_send_to_google_chat", lambda url, payload: sent.append((url, payload)))
    return table, sent


def _record(message_id, webhook_key="team"):
    body = json.dumps({"webhookKey": webhook_key, "payload": {"text": "hello"}})
    return {"messageId": message_id, "body": body}


def test_sends_and_records_completion(patch_deps):
    table, sent = patch_deps

    resp = handler.lambda_handler({"Records": [_record("m1")]}, None)

    assert resp == {"batchItemFailures": []}
    assert sent == [("https://chat.example/hook", {"text": "hello"})]
    assert table.items["m1"]["status"] == "COMPLETED"


def test_duplicate_message_is_skipped(patch_deps):
    table, sent = patch_deps

    handler.lambda_handler({"Records": [_record("m1")]}, None)
    resp = handler.lambda_handler({"Records": [_record("m1")]}, None)

    assert resp == {"batchItemFailures": []}
    assert len(sent) == 1


def test_failure_is_reported_and_not_recorded(patch_deps):
    table, sent = patch_deps

    resp = handler.lambda_handler({"Records": [_record("m1", webhook_key="unknown"), _record("m2")]}, None)

    assert resp == {"batchItemFailures": [{"itemIdentifier": "m1"}]}
    assert "m1" not in table.items
    assert "m2" in table.items
