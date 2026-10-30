"""Send explicitly synthetic OTLP/HTTP JSON observations. Python standard library only."""
import argparse
import heapq
import itertools
import json
import os
import secrets
import time
import urllib.error
import urllib.request


def span(topic, operation, offset, producer=None, error=False):
    now = time.time_ns()
    result = {"traceId": secrets.token_hex(16), "spanId": secrets.token_hex(8),
              "name": f"{topic} {operation}", "kind": 4 if operation == "publish" else 5,
              "startTimeUnixNano": str(now - 8_000_000), "endTimeUnixNano": str(now),
              "status": {"code": 2 if error else 1},
              "attributes": [{"key": key, "value": {"stringValue": value}} for key, value in [
                  ("messaging.system", "kafka"), ("messaging.operation", operation),
                  ("messaging.destination.name", topic), ("messaging.kafka.destination.partition", "0"),
                  ("messaging.kafka.message.offset", str(offset))]]}
    if producer:
        result["links"] = [{"traceId": producer["traceId"], "spanId": producer["spanId"]}]
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", default="http://127.0.0.1:4318")
    parser.add_argument("--rate", type=float, default=3, help="Synthetic source records per second (0.1..1000)")
    parser.add_argument("--duration", type=float, default=30, help="Seconds to run; 0 runs until interrupted")
    args = parser.parse_args()
    if not 0.1 <= args.rate <= 1000 or args.duration < 0:
        parser.error("Use a rate from 0.1 to 1000 and a nonnegative duration")
    headers = {"Content-Type": "application/json"}
    if os.environ.get("LIVEFLOW_API_KEY"):
        headers["Authorization"] = "Bearer " + os.environ["LIVEFLOW_API_KEY"]
    pending = []
    serial = itertools.count()
    offset = time.time_ns() // 1_000_000
    start = next_record = time.monotonic()
    sent = 0

    def schedule(delay, service, topic, operation, record, group=None, producer=None, error=False):
        heapq.heappush(pending, (time.monotonic() + delay, next(serial), service, topic, operation, record, group, producer, error))

    print("Synthetic demo only: environment=demo, cluster=demo-kafka. No Kafka broker is contacted.")
    try:
        while True:
            now = time.monotonic()
            producing = args.duration == 0 or now - start < args.duration
            if producing and now >= next_record:
                offset += 1
                publish = span("orders.created", "publish", offset)
                heapq.heappush(pending, (now, next(serial), "orders-api", "orders.created", "publish", offset, None, publish, False))
                schedule(.18, "payments", "orders.created", "receive", offset, "payments-v1", publish)
                schedule(2.4 if offset % 5 == 0 else .5, "analytics", "orders.created", "receive", offset, "analytics-v2", publish)
                downstream = span("payments.authorized", "publish", offset)
                heapq.heappush(pending, (now + .65, next(serial), "payments", "payments.authorized", "publish", offset, None, downstream, False))
                schedule(.9, "fulfilment", "payments.authorized", "receive", offset, "fulfilment-v1", downstream)
                next_record = now + 1 / args.rate
            if not producing and not pending:
                break
            exports = []
            while pending and pending[0][0] <= now:
                _, _, service, topic, operation, record, group, producer, error = heapq.heappop(pending)
                event = producer if operation == "publish" else span(topic, operation, record, producer, error)
                if operation == "publish":
                    # Keep IDs for correlation, but date the observed completion at actual send time.
                    timestamp = time.time_ns()
                    event["startTimeUnixNano"] = str(timestamp - 8_000_000)
                    event["endTimeUnixNano"] = str(timestamp)
                if group:
                    event["attributes"].append({"key": "messaging.kafka.consumer.group", "value": {"stringValue": group}})
                exports.append({"resource": {"attributes": [{"key": key, "value": {"stringValue": value}} for key, value in [
                    ("service.name", service), ("service.namespace", "shop"), ("deployment.environment.name", "demo"), ("kafka.cluster.id", "demo-kafka")]]},
                    "scopeSpans": [{"scope": {"name": "liveflow.synthetic-demo"}, "spans": [event]}]})
            if exports:
                request = urllib.request.Request(args.url.rstrip("/") + "/v1/traces", json.dumps({"resourceSpans": exports}).encode(), headers=headers)
                with urllib.request.urlopen(request, timeout=10) as response:
                    result = json.load(response)
                    if result.get("partialSuccess", {}).get("rejectedSpans", "0") not in ("0", 0):
                        raise RuntimeError("The service rejected observations: " + str(result))
                sent += len(exports)
            time.sleep(.01)
    except KeyboardInterrupt:
        pass
    print(f"Sent {sent} synthetic client-operation observations.")


if __name__ == "__main__":
    main()
