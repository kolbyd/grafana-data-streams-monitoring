"""Post one explicitly synthetic standard-OTel fixture, with fresh IDs and timestamps."""
import argparse
import json
import os
from pathlib import Path
import secrets
import time
import urllib.error
import urllib.request


def refreshed(document):
    spans = [span for resource in document["resourceSpans"]
             for scope in resource["scopeSpans"] for span in scope["spans"]]
    traces = {}
    identifiers = {}

    def trace(value):
        return traces.setdefault(value, secrets.token_hex(16))

    def span_id(trace_id, value):
        return identifiers.setdefault((trace_id, value), secrets.token_hex(8))

    first = min(int(span["startTimeUnixNano"]) for span in spans)
    delta = time.time_ns() - 100_000_000 - first
    for span in spans:
        old_trace = span["traceId"]
        old_span = span["spanId"]
        span["traceId"] = trace(old_trace)
        span["spanId"] = span_id(old_trace, old_span)
        if span.get("parentSpanId"):
            span["parentSpanId"] = span_id(old_trace, span["parentSpanId"])
        for link in span.get("links", []):
            link_trace = link["traceId"]
            link["traceId"] = trace(link_trace)
            link["spanId"] = span_id(link_trace, link["spanId"])
        span["startTimeUnixNano"] = str(int(span["startTimeUnixNano"]) + delta)
        span["endTimeUnixNano"] = str(int(span["endTimeUnixNano"]) + delta)
    return document


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", default="http://127.0.0.1:4318/v1/traces")
    parser.add_argument("--fixture", choices=["standard-chain.otlp.json", "cross-trace-chain.otlp.json"], default="cross-trace-chain.otlp.json")
    parser.add_argument("--key-env", help="Optional environment-variable name containing the service API key")
    args = parser.parse_args()
    source = Path(__file__).resolve().parents[1] / "examples" / "otel" / args.fixture
    document = refreshed(json.loads(source.read_text(encoding="utf-8")))
    headers = {"Content-Type": "application/json"}
    if args.key_env:
        key = os.environ.get(args.key_env)
        if not key:
            parser.error("The requested API-key environment variable is empty")
        headers["Authorization"] = "Bearer " + key
    request = urllib.request.Request(args.url, data=json.dumps(document).encode("utf-8"), headers=headers, method="POST")
    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            result = json.loads(response.read() or b"{}")
            print(json.dumps({"synthetic": True, "fixture": args.fixture, "httpStatus": response.status,
                              "illustrativePathwayMs": 45, "response": result}))
    except urllib.error.HTTPError as error:
        raise SystemExit("Synthetic fixture upload failed: HTTP " + str(error.code)) from None
    except urllib.error.URLError:
        raise SystemExit("Synthetic fixture upload failed: endpoint unavailable") from None


if __name__ == "__main__":
    main()
