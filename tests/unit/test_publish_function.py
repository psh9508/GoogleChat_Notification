import base64
import json

import pytest

import publish_function.app as handler


class DummySQS:
    def __init__(self):
        self.calls = []

    def send_message(self, QueueUrl, MessageBody):
        self.calls.append({"QueueUrl": QueueUrl, "MessageBody": MessageBody})
        return {"MessageId": "msg-123"}


@pytest.fixture(autouse=True)
def patch_sqs(monkeypatch):
    dummy = DummySQS()
    monkeypatch.setattr(handler, "sqs", dummy)
    return dummy


def test_valid_json_payload(patch_sqs):
    event = {
        "body": json.dumps({"key": "team", "payload": {"text": "hello"}}),
        "isBase64Encoded": False,
    }

    resp = handler.lambda_handler(event, None)

    assert resp["statusCode"] == 200
    assert patch_sqs.calls[0]["MessageBody"] == json.dumps({"text": "hello"})


def test_invalid_json_body():
    event = {"body": "not-json"}
    resp = handler.lambda_handler(event, None)
    assert resp["statusCode"] == 400


def test_missing_fields():
    event = {"body": json.dumps({"key": "team"})}
    resp = handler.lambda_handler(event, None)
    assert resp["statusCode"] == 400


def test_payload_must_be_json_string_or_object():
    event = {"body": json.dumps({"key": "team", "payload": "not-json-string"})}
    resp = handler.lambda_handler(event, None)
    assert resp["statusCode"] == 400


def test_base64_body_decoding(patch_sqs):
    raw = json.dumps({"key": "team", "payload": {"text": "ok"}}).encode("utf-8")
    event = {"body": base64.b64encode(raw).decode("utf-8"), "isBase64Encoded": True}

    resp = handler.lambda_handler(event, None)

    assert resp["statusCode"] == 200
    assert patch_sqs.calls[0]["MessageBody"] == json.dumps({"text": "ok"})
