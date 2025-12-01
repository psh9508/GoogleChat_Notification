import json
import logging
import os
import time

import boto3
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

secrets_client = boto3.client("secretsmanager")
SECRET_ARN = os.environ["SECRET_ARN"]

LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)

CACHED_WEBHOOKS = None
LAST_SECRET_FETCH = 0.0
SECRET_CACHE_TTL = float(os.getenv("SECRET_CACHE_TTL", "60"))
TIMEOUT_SECONDS = float(os.getenv("GOOGLE_CHAT_TIMEOUT", "5"))
RETRY_TOTAL = int(os.getenv("GOOGLE_CHAT_RETRIES", "2"))
BACKOFF_FACTOR = float(os.getenv("GOOGLE_CHAT_BACKOFF", "0.5"))


def _build_http_session():
    retry = Retry(
        total=RETRY_TOTAL,
        status_forcelist=[429, 500, 502, 503, 504],
        backoff_factor=BACKOFF_FACTOR,
        allowed_methods=frozenset(["POST"]),
    )
    adapter = HTTPAdapter(max_retries=retry)
    session = requests.Session()
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


http_session = _build_http_session()


def get_all_webhooks():
    global CACHED_WEBHOOKS, LAST_SECRET_FETCH

    now = time.time()
    if CACHED_WEBHOOKS and (now - LAST_SECRET_FETCH) < SECRET_CACHE_TTL:
        return CACHED_WEBHOOKS

    LOGGER.info("Fetching secrets from AWS Secrets Manager...")
    try:
        response = secrets_client.get_secret_value(SecretId=SECRET_ARN)
        secret_string = response["SecretString"]
        CACHED_WEBHOOKS = json.loads(secret_string)
        LAST_SECRET_FETCH = now
        return CACHED_WEBHOOKS
    except Exception as exc:
        LOGGER.error("Error fetching secret: %s", exc)
        raise


def _parse_payload(body: str) -> dict:
    payload = json.loads(body)
    key = payload.get("key")
    text = payload.get("text")
    if not key or text is None:
        raise ValueError("payload must include 'key' and 'text'")
    return {"key": key, "text": text}


def _resolve_webhook(key: str, webhook_map: dict) -> str:
    if key not in webhook_map:
        raise KeyError(f"Key '{key}' not found in secrets")
    return webhook_map[key]


def _send_to_google_chat(url: str, message_body):
    resp = http_session.post(url, json=message_body, timeout=TIMEOUT_SECONDS)
    resp.raise_for_status()


def lambda_handler(event, context):
    batch_item_failures = []

    webhook_map = get_all_webhooks()

    for record in event.get("Records", []):
        message_id = record.get("messageId")
        try:
            payload = _parse_payload(record["body"])
            target_url = _resolve_webhook(payload["key"], webhook_map)
            _send_to_google_chat(target_url, payload["text"])
            LOGGER.info("Message sent to key: %s", payload["key"])
        except KeyError as exc:
            LOGGER.warning("Skipping message %s: %s", message_id, exc)
        except Exception as exc:
            LOGGER.error("Failed to process message %s: %s", message_id, exc)
            batch_item_failures.append({"itemIdentifier": message_id})

    return {"batchItemFailures": batch_item_failures}
