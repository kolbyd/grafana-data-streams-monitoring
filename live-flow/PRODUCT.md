# Live Flow

<!-- impeccable:product-schema 1 -->

## Platform

web

## Stack

The user uses Confluent.Kafka for .NET, self-hosted Kafka, and Grafana Cloud. They authorized implementation of the live flow service and will review the completed result. Implementation choices: ASP.NET Core 10, OTLP/HTTP, and a dependency-free JavaScript/SVG review viewer. These implementation choices are assumptions made within that authorization, not a separate user technology preference.

## Users

Engineers investigating event-driven services who want to see producer/topic/consumer relationships and observed message activity automatically.

## Product Purpose

Build a live event pipeline from standard OTel Kafka instrumentation and show real observations in a topology with animated flow.

## Operating Context

The local viewer is a review and diagnostic companion. The service exposes a documented streaming protocol for a future Grafana integration. No deployment or Grafana changes have been requested.

## Capabilities and Constraints

Observe publish and receive boundaries, correlate Kafka hops, preserve consumer-group identities on edges, and recover after reconnects. Never imply that receiving a message proves completed business processing or that missing observations prove backlog. State is bounded and in memory. Synthetic traffic must be clearly labeled and explicitly started.

## Evidence on Hand

No access to the user's running Kafka clients or representative production telemetry has been provided. Demo observations are synthetic. No production throughput or latency guarantees are established.

## Product Principles

- Show observed facts and distinguish correlation from inference.
- Keep topology stable while activity moves.
- Preserve counts when reducing animation detail.
- Make loss, resets, and missing context visible.
