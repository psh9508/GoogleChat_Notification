import base64
import json
import os

import boto3

sqs = boto3.client("sqs")
QUEUE_URL = os.environ["QUEUE_URL"]


def lambda_handler(event, context):
    try:
        body_str = _extract_body(event)
        if body_str is None:
            return _response(400, {"message": "request body is required"})

        try:
            body = json.loads(body_str)
        except json.JSONDecodeError:
            return _response(400, {"message": "body must be valid JSON"})

        key = body.get("key")
        payload = body.get("payload")
        if not key or payload is None:
            return _response(400, {"message": "fields 'key' and 'payload' are required"})

        message_body = _coerce_json_payload(payload)
        if message_body is None:
            return _response(
                400, {"message": "field 'payload' must be a JSON object/array or JSON string"}
            )

        response = sqs.send_message(
            QueueUrl=QUEUE_URL,
            MessageBody=message_body,
        )

        return _response(200, {"message": "queued", "id": response["MessageId"]})
    except Exception as exc:  # pragma: no cover - surfaced via API
        print(exc)
        return _response(500, {"message": "internal error"})


def _extract_body(event):
    body = event.get("body")
    if body is None:
        return None
    if event.get("isBase64Encoded"):
        try:
            body = base64.b64decode(body).decode("utf-8")
        except Exception:
            return None
    return body


def _coerce_json_payload(value):
    if isinstance(value, (dict, list)):
        return json.dumps(value)
    if isinstance(value, str):
        try:
            json.loads(value)
            return value
        except json.JSONDecodeError:
            return None
    return None


def _response(status_code, payload):
    return {
        "statusCode": status_code,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps(payload),
    }
