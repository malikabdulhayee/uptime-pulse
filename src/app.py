import os
import json
import time
import uuid
import urllib.request
import urllib.error
from decimal import Decimal
from datetime import datetime, timezone

import boto3

dynamodb = boto3.resource("dynamodb")
sns = boto3.client("sns")
sfn = boto3.client("stepfunctions")

TABLE_NAME = os.environ.get("TABLE_NAME")
SNS_TOPIC_ARN = os.environ.get("SNS_TOPIC_ARN")
STATE_MACHINE_ARN = os.environ.get("STATE_MACHINE_ARN")

HISTORY_TTL_SECONDS = 7 * 24 * 60 * 60


def _now():
    return datetime.now(timezone.utc).isoformat()


def _json_default(o):
    if isinstance(o, Decimal):
        return int(o) if o == o.to_integral_value() else float(o)
    return str(o)


def _resp(code, body, extra_headers=None):
    headers = {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"}
    if extra_headers:
        headers.update(extra_headers)
    return {"statusCode": code, "headers": headers, "body": json.dumps(body, default=_json_default)}


def _query_all_urls():
    """Query the GSI (no table scans) for every registered URL."""
    table = dynamodb.Table(TABLE_NAME)
    items = []
    kwargs = {
        "IndexName": "GSI1",
        "KeyConditionExpression": "GSI1PK = :m",
        "ExpressionAttributeValues": {":m": "META"},
    }
    while True:
        r = table.query(**kwargs)
        items.extend(r.get("Items", []))
        if "LastEvaluatedKey" not in r:
            break
        kwargs["ExclusiveStartKey"] = r["LastEvaluatedKey"]
    return items


# ---------- Step Functions task: ping ----------
def ping_handler(event, context):
    """Never raises: any failure (HTTP error, DNS, refused, timeout) becomes is_up=False."""
    url = event["url"]
    start = time.time()
    status = "0"
    is_up = False
    error = ""
    try:
        req = urllib.request.Request(url, method="GET", headers={"User-Agent": "UptimePulse/1.0"})
        with urllib.request.urlopen(req, timeout=8) as resp:
            code = resp.getcode()
            status = str(code)
            is_up = 200 <= code < 400
    except urllib.error.HTTPError as e:
        status = str(e.code)
        is_up = False
    except Exception as e:  # DNS failure, refused, timeout, SSL...
        status = "0"
        is_up = False
        error = str(e)[:200]
    latency = int((time.time() - start) * 1000)  # int: DynamoDB rejects float
    return {
        "url": url,
        "is_up": is_up,
        "status": status,
        "error": error,
        "latency_ms": latency,
        "checked_at": _now(),
    }


# ---------- Step Functions task: record + alert on state change ----------
def _fmt_duration(seconds):
    seconds = int(seconds)
    m, s = divmod(seconds, 60)
    h, m = divmod(m, 60)
    if h:
        return f"{h}h {m}m {s}s"
    if m:
        return f"{m}m {s}s"
    return f"{s}s"


def process_handler(event, context):
    table = dynamodb.Table(TABLE_NAME)
    url = event["url"]
    is_up = bool(event["is_up"])
    status = event["status"]
    latency = int(event["latency_ms"])
    checked_at = event["checked_at"]
    error = event.get("error", "")

    prev = table.get_item(Key={"pk": f"URL#{url}", "sk": "LATEST"}).get("Item", {})
    prev_is_up = prev.get("is_up", True)  # new site starts as "up", so a first failure alerts
    prev_down_since = prev.get("down_since")

    down_since = None
    if prev_is_up and not is_up:
        down_since = checked_at
        sns.publish(
            TopicArn=SNS_TOPIC_ARN,
            Subject=f"UptimePulse: {url} is DOWN"[:100],
            Message=f"DOWN: {url}\nHTTP status: {status}\nDetail: {error or 'n/a'}\nTime: {checked_at}\n(confirmed by a recheck after 20s)",
        )
    elif not prev_is_up and not is_up:
        down_since = prev_down_since  # still down: no new alert
    elif not prev_is_up and is_up:
        duration = "unknown"
        if prev_down_since:
            delta = datetime.fromisoformat(checked_at) - datetime.fromisoformat(prev_down_since)
            duration = _fmt_duration(delta.total_seconds())
        sns.publish(
            TopicArn=SNS_TOPIC_ARN,
            Subject=f"UptimePulse: {url} RECOVERED"[:100],
            Message=f"RECOVERED: {url}\nDowntime: {duration}\nTime: {checked_at}",
        )

    latest = {
        "pk": f"URL#{url}",
        "sk": "LATEST",
        "GSI1PK": "META",
        "GSI1SK": f"URL#{url}",
        "url": url,
        "is_up": is_up,
        "status": status,
        "latency_ms": latency,
        "checked_at": checked_at,
    }
    if down_since:
        latest["down_since"] = down_since
    table.put_item(Item=latest)

    table.put_item(Item={
        "pk": f"URL#{url}",
        "sk": f"CHECK#{checked_at}",
        "url": url,
        "is_up": is_up,
        "status": status,
        "latency_ms": latency,
        "checked_at": checked_at,
        "ttl": int(time.time()) + HISTORY_TTL_SECONDS,
    })

    return {"url": url, "was_up": prev_is_up, "is_up": is_up}


# ---------- EventBridge schedule: start one execution per URL ----------
def dispatcher_handler(event, context):
    items = _query_all_urls()
    started = 0
    for item in items:
        url = item["url"]
        try:
            sfn.start_execution(
                stateMachineArn=STATE_MACHINE_ARN,
                name=f"check-{int(time.time())}-{uuid.uuid4().hex[:8]}",
                input=json.dumps({"url": url}),
            )
            started += 1
        except Exception as e:
            print(f"Failed to start execution for {url}: {e}")
    return {"registered": len(items), "started": started}


# ---------- API: POST /register (API key required) ----------
def register_handler(event, context):
    try:
        body = event.get("body") or "{}"
        if isinstance(body, str):
            body = json.loads(body)
        url = (body.get("url") or "").strip()
        if not url.startswith(("http://", "https://")) or len(url) > 500:
            return _resp(400, {"error": "Provide a valid 'url' starting with http:// or https://"})

        table = dynamodb.Table(TABLE_NAME)
        try:
            table.put_item(
                Item={
                    "pk": f"URL#{url}",
                    "sk": "LATEST",
                    "GSI1PK": "META",
                    "GSI1SK": f"URL#{url}",
                    "url": url,
                    "is_up": True,
                    "status": "pending",
                    "latency_ms": 0,
                    "checked_at": _now(),
                },
                ConditionExpression="attribute_not_exists(pk)",
            )
        except dynamodb.meta.client.exceptions.ConditionalCheckFailedException:
            return _resp(200, {"message": f"Already monitoring {url}"})
        return _resp(200, {"message": f"Now monitoring {url}"})
    except Exception as e:
        print(f"register error: {e}")
        return _resp(500, {"error": "Internal error"})


# ---------- API: GET /status ----------
def status_handler(event, context):
    items = _query_all_urls()
    items.sort(key=lambda x: x.get("url", ""))
    out = [{
        "url": i.get("url"),
        "is_up": i.get("is_up"),
        "status": i.get("status"),
        "latency_ms": i.get("latency_ms"),
        "checked_at": i.get("checked_at"),
        "down_since": i.get("down_since"),
    } for i in items]
    return _resp(200, out)


# ---------- API: GET / (serves the status page) ----------
def frontend_handler(event, context):
    path = os.path.join(os.path.dirname(__file__), "index.html")
    with open(path, "r", encoding="utf-8") as f:
        html = f.read()
    return {"statusCode": 200, "headers": {"Content-Type": "text/html; charset=utf-8"}, "body": html}
