import json
import logging
import os
import time

import boto3
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from botocore.exceptions import ClientError

# --- Configuration and Initialization ---
LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)

# Environment Variables
SECRET_ARN = os.environ["SECRET_ARN"]
# Use the table name passed as an environment variable from the SAM template
TABLE_NAME = os.environ["DYNAMODB_TABLE_NAME"] 
SECRET_CACHE_TTL = float(os.getenv("SECRET_CACHE_TTL", "60"))
TIMEOUT_SECONDS = float(os.getenv("GOOGLE_CHAT_TIMEOUT", "5"))
RETRY_TOTAL = int(os.getenv("GOOGLE_CHAT_RETRIES", "2"))
BACKOFF_FACTOR = float(os.getenv("GOOGLE_CHAT_BACKOFF", "0.5"))

# Global clients (Optimization for Lambda Cold Start)
secrets_client = boto3.client("secretsmanager")
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table(TABLE_NAME)

# Caching variables
CACHED_WEBHOOKS = None
LAST_SECRET_FETCH = 0.0

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


def _resolve_webhook(key: str, webhook_map: dict) -> str:
    if key not in webhook_map:
        raise KeyError(f"Key '{key}' not found in secrets")
    return webhook_map[key]


def _send_to_google_chat(url: str, payload):    
    if isinstance(payload, (dict, list)):
        message_body = payload
    else:
        message_body = {"text": str(payload)}

    resp = http_session.post(url, json=message_body, timeout=TIMEOUT_SECONDS)
    resp.raise_for_status()


def process_record_idempotent(record):
    message_id = record.get("messageId")
    body_str = record.get("body")
    LOGGER.info("Processing message_id=%s body_preview=%s", message_id, str(body_str)[:300])
    
    try:
        table.put_item(
            Item={
                'PK': message_id,
                'status': 'IN_PROGRESS',
                'created_at': int(time.time()),
                'ttl': int(time.time()) + 120
            },
            ConditionExpression='attribute_not_exists(message_id)'
        )
    except ClientError as e:
        if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
            LOGGER.info(f"Duplicate Message detected: {message_id}. Skipping.")
            return
        else:
            # Raise DB connection errors etc., to trigger SQS retry
            raise e

    try:
        LOGGER.info(f"Processing message: {message_id}")
        body = json.loads(body_str)
        
        webhook_map = get_all_webhooks()
        target_url = _resolve_webhook(body["webhookKey"], webhook_map)
        
        _send_to_google_chat(target_url, body["payload"])
        LOGGER.info("Message sent to webhookKey: %s", body["webhookKey"])

        table.update_item(
            Key={'PK': message_id},
            UpdateExpression="set #st = :st_val, #ttl = :ttl_val",
            ExpressionAttributeNames={'#st': 'status', '#ttl': 'ttl'},
            ExpressionAttributeValues={
                ':st_val': 'COMPLETED',
                ':ttl_val': int(time.time()) + 86400,  # expire after 24h
            }
        )
        LOGGER.info(f"Successfully completed: {message_id}")

    except Exception as exc:
        LOGGER.error(
            "Failed to process message_id=%s error=%s body_preview=%s",
            message_id,
            exc,
            str(body_str)[:500],
            exc_info=True,
        )
        try:
            table.delete_item(Key={'PK': message_id})
        except Exception as delete_exc:
            LOGGER.error(f"Failed to rollback lock for {message_id}: {delete_exc}")
        
        raise exc

def lambda_handler(event, context):
    batch_item_failures = []

    for record in event.get("Records", []):
        message_id = record.get("messageId")
        try:
            process_record_idempotent(record)
            
        except Exception as exc:
            # Error is raised after Rollback inside process_record_idempotent
            # Notify SQS: "This message failed, please provide it again later"
            LOGGER.error(
                "Message %s failed permanently in this run. error=%s body_preview=%s",
                message_id,
                exc,
                str(record.get('body'))[:500],
                exc_info=True,
            )
            batch_item_failures.append({"itemIdentifier": message_id})

    return {"batchItemFailures": batch_item_failures}
