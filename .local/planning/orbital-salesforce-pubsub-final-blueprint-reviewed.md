# Orbital Salesforce Platform Events Connector
## Final Open-Source Library and OrbitalHQ Connector Blueprint

> **Repository:** `orbital-salesforce-pubsub`  
> **Primary outcome:** A focused open-source JVM library and OrbitalHQ connector for consuming and publishing Salesforce **custom Platform Events** through Salesforce Pub/Sub API.  
> **Primary distribution:** Maven Central release artifacts; GitHub Packages optionally for snapshots/internal builds.  
> **v0.1 focus:** Salesforce Platform Events ↔ OrbitalHQ only. No DynamoDB, Kafka, S3, HTTP sink, ETL pipeline, or other downstream connector is owned by this project.

---

# 1. Goal and Scope

Build a community connector that makes Salesforce Platform Events available in OrbitalHQ as native streams and lets OrbitalHQ publish custom Platform Events.

## 1.1 Required capabilities

### Consume

```text
Salesforce custom Platform Event
        ↓
Salesforce Pub/Sub API (gRPC / HTTP/2)
        ↓
orbital-salesforce-pubsub
        ↓
OrbitalHQ Stream<T>
```

### Publish

```text
OrbitalHQ write operation
        ↓
orbital-salesforce-pubsub
        ↓
Salesforce Pub/Sub API (gRPC / HTTP/2)
        ↓
Salesforce custom Platform Event
```

## 1.2 Explicit non-goals

The project does **not** implement or own:

```text
DynamoDB integrations
Kafka integrations
SQS integrations
databases or file sinks
HTTP destinations
ETL pipelines
business transformations
destination-specific idempotency code
CDC-specific helpers
Event Monitoring helpers
legacy CometD / Streaming API support
ManagedSubscribe in v0.1
PublishStream in v0.1
batch publishing in v0.1
Salesforce schema auto-generation into Taxi
an Orbital connection UI wizard
```

A user may compose the Salesforce stream with other Orbital services in TaxiQL, but that composition is outside the connector’s code and responsibility.

---

# 2. Design Principles

1. **Use Salesforce Pub/Sub API only in v0.1.** It is Salesforce’s gRPC/HTTP/2 interface for external publishing and subscribing to Platform Events. Do not introduce CometD compatibility abstractions unless there is a demonstrated requirement.

2. **Keep Salesforce protocol logic independent from OrbitalHQ.** The reusable client must not depend on Orbital, Taxi, Spring, or a particular downstream target.

3. **Expose consumption as `Stream<T>`.** A Platform Event subscription is a live source, never a polling list or blocking iterator in the Orbital adapter.

4. **Expose publishing as an explicit write operation.** Do not model event publishing as a read operation or implicit side effect.

5. **Keep semantics separate from connectivity.** Taxi describes what the event means; connection configuration describes OAuth, endpoint, retries, flow-control defaults, and secrets.

6. **Use semantic types across source boundaries.** Reuse scalar types such as `OrderId` or `CustomerId`; do not make a Salesforce physical event model the enterprise’s universal domain model.

7. **Never treat replay IDs as event IDs.** A replay ID is an opaque stream position. Keep it as `byte[]` internally; do not increment it, cast it to a number, or use it as a business key.

8. **Use Salesforce event ID as the default event identity when available; never use replay ID as a business identifier.**

9. **Do not overpromise delivery semantics.** The v0.1 connector must never claim exactly-once delivery. Its documented behavior must match what Orbital’s current source-connector SPI can actually acknowledge and checkpoint.

10. **Keep v0.1 small and operable.** First prove real Salesforce Pub/Sub subscribe and publish with the standalone CLI, then add the thin Orbital adapter.

---

# 3. Project Architecture

```text
                               Salesforce Event Bus
                                      │
                     ┌────────────────┴────────────────┐
                     │                                 │
                     │ Subscribe                       │ Publish
                     ▼                                 ▲
┌─────────────────────────────────────────────────────────────────────┐
│ salesforce-pubsub-core                                               │
│                                                                     │
│ - OAuth/session provider abstraction                                │
│ - gRPC channel and required metadata                               │
│ - GetTopic / GetSchema                                               │
│ - bounded Avro schema cache                                          │
│ - Avro decode and encode                                             │
│ - Platform Event subscription                                        │
│ - flow control and bounded prefetch                                 │
│ - replay start and replay-store SPI                                 │
│ - reconnect, retry, token refresh                                   │
│ - typed exceptions, lifecycle, telemetry                            │
│ - unary Publish()                                                    │
└──────────────────────────────┬──────────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────────┐
│ orbital-salesforce-connector                                        │
│                                                                     │
│ - Taxi annotations                                                   │
│ - named connection configuration mapping                            │
│ - Orbital OperationInvoker integration                              │
│ - SalesforceEvent → TypedInstance                                   │
│ - TypedInstance → publish payload                                   │
│ - Stream<T> lifecycle and cancellation handling                     │
│ - Orbital tracing, metrics, and connector health                    │
└─────────────────────────────────────────────────────────────────────┘
```

## 3.1 Maven modules

```text
orbital-salesforce-pubsub/
├── salesforce-pubsub-core/
├── salesforce-pubsub-cli/
├── orbital-salesforce-connector/
├── bom/
├── examples/
├── docs/
│   ├── architecture.md
│   ├── configuration.md
│   ├── replay-and-delivery.md
│   ├── operations.md
│   └── troubleshooting.md
├── .github/
│   ├── workflows/
│   ├── ISSUE_TEMPLATE/
│   └── pull_request_template.md
├── LICENSE
├── NOTICE
├── README.md
├── CONTRIBUTING.md
├── SECURITY.md
├── CODE_OF_CONDUCT.md
├── CHANGELOG.md
└── pom.xml
```

## 3.2 Artifact coordinates

```text
io.github.<org>:salesforce-pubsub-core
io.github.<org>:salesforce-pubsub-cli
io.github.<org>:orbital-salesforce-pubsub-connector
io.github.<org>:orbital-salesforce-pubsub-bom
```

Publish releases to Maven Central. Optionally publish snapshots to GitHub Packages.

---

# 4. Technology Decisions

## 4.1 Runtime and language

```text
Java:   21
Build:  Maven
Core:   Java 21, no Orbital dependency
Adapter: Kotlin, OrbitalHQ and Taxi dependencies only
Protocol: Salesforce Pub/Sub API, gRPC, HTTP/2, Protocol Buffers, Avro
Logging: SLF4J
```

## 4.2 Salesforce protocol scope

Generate Java stubs from Salesforce’s Pub/Sub proto during the Maven build.

Required v0.1 RPCs:

```text
GetTopic
GetSchema
Subscribe
Publish
```

Deferred:

```text
ManagedSubscribe
PublishStream
```

Keep generated gRPC/proto types internal to the transport implementation. Do not expose Salesforce-generated classes in the stable public core API.

## 4.3 Salesforce compatibility statement

The README must say clearly:

```text
This connector requires Salesforce Pub/Sub API availability and an
integration principal with the required API and Platform Event permissions.
```

Do not claim compatibility with orgs, editions, or deployments not validated by real integration tests.

---

# 5. Core Library Design

## 5.1 Package structure

```text
io.github.<org>.salesforce.pubsub
├── auth/
├── config/
├── transport/
├── schema/
├── subscription/
├── replay/
├── publish/
├── model/
├── telemetry/
└── error/
```

Recommended classes:

```text
auth/
  SalesforceAuthProvider
  SalesforceSession
  ClientCredentialsAuthProvider
  UserSuppliedAuthProvider

config/
  SalesforcePubSubConfig
  FlowControlOptions
  RetryOptions
  EndpointConfig

transport/
  SalesforceEventTransport
  PubSubApiTransport
  SalesforceCallCredentials

schema/
  SalesforceAvroCodec
  SalesforceSchemaCache
  SchemaResolver

subscription/
  SubscribeRequest
  Subscription
  SubscriptionStart
  SubscriptionContext
  SubscriptionLifecycleEvent
  SalesforceEventHandler
  SubscriptionState

replay/
  ReplayStore
  InMemoryReplayStore
  InvalidReplayPolicy

publish/
  PublishRequest
  PublishReceipt

model/
  SalesforceEvent

telemetry/
  SalesforcePubSubTelemetry
  NoOpSalesforcePubSubTelemetry

error/
  SalesforcePubSubException
  AuthenticationException
  AuthorizationException
  TopicNotFoundException
  SubscriptionException
  InvalidReplayException
  SchemaLookupException
  EventDecodeException
  EventEncodeException
  PublishException
  ReplayStoreException
  TransportException
```

## 5.2 Public client API

Use an asynchronous API. The adapter can convert it to the reactive/coroutine form required by the installed Orbital version.

```java
public interface SalesforcePubSubClient extends AutoCloseable {

    Subscription subscribe(
        SubscribeRequest request,
        SalesforceEventHandler handler
    );

    CompletionStage<PublishReceipt> publish(
        PublishRequest request
    );

    ConnectorStatus status();

    @Override
    void close();
}
```

Do not use blocking network calls on Orbital execution threads. Use asynchronous gRPC stubs. If a replay store is blocking, isolate it on a bounded I/O executor and preserve cancellation semantics.

## 5.3 Salesforce session/authentication

```java
public interface SalesforceAuthProvider {

    CompletionStage<SalesforceSession> authenticate();

    CompletionStage<SalesforceSession> refresh(
        SalesforceSession current
    );
}
```

```java
public record SalesforceSession(
    String accessToken,
    String instanceUrl,
    String tenantId,
    String userId
) {}
```

Implement in v0.1:

```text
ClientCredentialsAuthProvider
UserSuppliedAuthProvider
```

Design for later but do not implement initially:

```text
JWT bearer
Authorization code/browser login
Username/password grant
```

Authentication rules:

```text
- Never log access tokens, client secrets, refresh tokens, passwords, or authorization headers.
- Allow Orbital or an embedding application to provide a current Salesforce session.
- Refresh/re-authenticate only through the configured provider.
- Map authentication and authorization failures to separate typed exceptions.
```

## 5.4 gRPC metadata

Attach the Salesforce session values required by Pub/Sub API calls:

```text
accesstoken
instanceurl
tenantid
```

Implement with `CallCredentials` or a scoped interceptor that always reads the current session safely.

```java
public final class SalesforceCallCredentials extends CallCredentials {
    // Attaches current accesstoken, instanceurl, and tenantid metadata.
}
```

Rules:

```text
- New RPCs use refreshed session metadata after reauthentication.
- Metadata values are never written to application logs or exception messages.
- A session is not shared unsafely across concurrent refresh operations.
```

## 5.5 Topic validation

Before subscribing or publishing:

```text
GetTopic(topic)
```

Validate:

```text
- Topic exists.
- `can_subscribe` is true before Subscribe.
- `can_publish` is true before Publish.
- A schema ID is present when generic Avro serialization is needed.
```

Example custom Platform Event topic:

```text
/event/Manual_Message__e
```

Do not silently rewrite topic names or infer object names from invalid input.

## 5.6 Avro schema cache

The connector resolves a schema ID through `GetSchema`, parses the returned Avro schema, and caches it.

```text
schemaId
  → cache lookup
  → GetSchema on miss
  → parse Avro schema
  → decode ConsumerEvent payload or encode ProducerEvent payload
```

Recommended initial cache:

```text
Caffeine
maximumSize = 100
expireAfterAccess = 24 hours
key = schemaId
```

Requirements:

```text
- A new schema ID automatically triggers a new lookup.
- The cache is bounded.
- Cache failures do not poison later lookups indefinitely.
- Cache metrics report hits, misses, load failures, and evictions.
- Schema IDs are not assumed to identify a fixed Taxi model version.
```

## 5.7 Avro codec

Decode to neutral Java structures:

```java
Map<String, Object> decode(String schemaId, byte[] payload);
```

Encode from neutral Java structures:

```java
byte[] encode(String schemaId, Map<String, Object> payload);
```

Support:

```text
strings
booleans
integer and long values
floating point values
dates and timestamps
nulls
arrays
nested records
nullable unions
```

Normalize Avro-specific classes where needed:

```text
Utf8 → String
```

Error behavior:

```text
Decode failure → EventDecodeException
Encode failure → EventEncodeException
```

Safe exception context may include:

```text
topic
schemaId
eventId if available
hash or controlled representation of replay ID
gRPC status
```

Do not include full raw payloads at INFO/WARN or in ordinary exception messages.

## 5.8 Event model

```java
public record SalesforceEvent(
    String topic,
    String eventId,
    byte[] replayId,
    String schemaId,
    Instant receivedAt,
    Map<String, Object> payload
) {}
```

`byte[]` is mutable; make defensive copies at public API boundaries or wrap replay IDs in an immutable value type.

Replay ID rules:

```text
- Treat as opaque bytes internally.
- Never cast to long.
- Never increment, derive, compare numerically, or synthesize.
- Base64 encode only for a textual storage/configuration/Taxi boundary.
- Use Salesforce event ID as the default event identity when available.
- Never use replay ID as a business identifier.
```

---

# 6. Consume: Subscription, Replay, and Flow Control

## 6.1 Subscription start modes

```java
public sealed interface SubscriptionStart
    permits Latest, Earliest, ReplayId {}

public record Latest() implements SubscriptionStart {}
public record Earliest() implements SubscriptionStart {}
public record ReplayId(byte[] value) implements SubscriptionStart {}
```

Do not implement `ManagedSubscribe` in v0.1. It may be designed and evaluated later once its server-side commit behavior, permissions, and lifecycle are verified in real Salesforce environments.

## 6.2 Replay-start precedence

This behavior must be deterministic.

Use the following precedence:

```text
1. Explicit ReplayId supplied by the caller
   → start after that replay ID.

2. Existing ReplayStore checkpoint for
   connectionName + topic + consumerName
   → resume from that checkpoint.

3. No checkpoint exists
   → use configured replay preset:
      LATEST or EARLIEST.
```

Important rule:

```text
The replay preset is the initial-start policy.
It does not override an existing durable checkpoint.
```

This is conceptually similar to Kafka:

```text
known committed position
       ↓
resume

no committed position
       ↓
apply initial/reset policy
```

Salesforce replay IDs themselves are not Kafka offsets and must not be treated numerically.

## 6.3 Orbital-facing custom replay behavior

The core API and CLI must support explicit custom replay.

Core API:

```java
new ReplayId(byte[])
```

CLI:

```text
--replay-id <base64>
```

The Orbital Taxi annotation should not embed raw custom replay IDs.

Taxi exposes only the initial policy:

```taxi
enum SalesforceReplayPreset {
   latest,
   earliest
}
```

Normal Orbital restart/resume behavior should use:

```text
consumerName
+
ReplayStore
```

Do not add a raw replay token to Taxi source files in v0.1.

## 6.4 Replay policy

```java
public enum InvalidReplayPolicy {
    ERROR,
    EARLIEST,
    LATEST
}
```

Default:

```text
ERROR
```

Never silently reset an invalid replay ID to `LATEST` or `EARLIEST`. A reset may lose messages or unexpectedly replay a large history.

## 6.5 Replay store

```java
public interface ReplayStore {

    Optional<byte[]> load(
        String connectionName,
        String topic,
        String consumerName
    );

    void save(
        String connectionName,
        String topic,
        String consumerName,
        byte[] replayId
    );
}
```

Replay namespace:

```text
connectionName + topic + consumerName
```

Example:

```text
salesforce-prod:/event/Manual_Message__e:manual-message-consumer
```

Implement for v0.1:

```text
InMemoryReplayStore
```

A file-backed store is acceptable for CLI/local experimentation if it is clearly marked as single-process. Do not imply that it is suitable for HA production consumption.

Defer JDBC, DynamoDB, Redis, and other durable store implementations until a real deployment requirement exists.

## 6.6 Subscription request

```java
public record SubscribeRequest(
    String connectionName,
    String consumerName,
    String topic,
    SubscriptionStart start,
    InvalidReplayPolicy invalidReplayPolicy,
    FlowControlOptions flowControl
) {}
```

The consumer name must be stable across restarts when it identifies one logical pipeline. It must not be randomly generated for each process start.

## 6.7 Handler contract

The generic core does not invent a downstream acknowledgement model that the actual Orbital version cannot support.

```java
public interface SalesforceEventHandler {

    void onEvent(
        SubscriptionContext context,
        SalesforceEvent event
    );

    default void onLifecycleEvent(
        SubscriptionContext context,
        SubscriptionLifecycleEvent event
    ) {}

    default void onError(
        SubscriptionContext context,
        SalesforcePubSubException error
    ) {}
}
```

### Required investigation before durable Orbital checkpointing

Before claiming any checkpoint behavior in the Orbital adapter, inspect the current Orbital source connector implementations and determine:

```text
- How Kafka source offsets are committed.
- How SQS source messages are acknowledged/deleted.
- Whether downstream TaxiQL completion/failure reaches OperationInvoker.
- Whether source stream APIs expose a delivery acknowledgement callback.
- How stream cancellation and errors propagate.
```

Then implement and document the strongest behavior Orbital actually permits.

### Delivery language for v0.1

Until that investigation proves an end-to-end acknowledgement hook, state:

```text
The connector does not provide exactly-once delivery.
Replay behavior and any checkpoint behavior are documented according to the
actual Orbital connector SPI available in the supported Orbital release.
```

If Orbital provides a downstream-success acknowledgement boundary, adopt this rule:

```text
Only persist a replay checkpoint after Orbital reports the event has completed
successfully through the supported acknowledgement mechanism.
```

If Orbital does not provide such an acknowledgement boundary, do not pretend that an arbitrary callback return means a downstream destination committed successfully.

## 6.8 Flow control

Salesforce subscriptions use client-controlled demand. Use bounded prefetch for v0.1.

```java
public record FlowControlOptions(
    int initialRequestCount,
    int refillThreshold,
    int refillCount,
    int maxInFlightEvents
) {
    public static FlowControlOptions defaults() {
        return new FlowControlOptions(10, 2, 10, 20);
    }
}
```

Required behavior:

```text
Subscribe
  → request initial event count
  → track events in flight
  → refill when the threshold is crossed
  → never exceed maxInFlightEvents
```

Never request more events after:

```text
Orbital stream cancellation
connector shutdown
terminal failure
subscription close
```

The Orbital adapter must inspect the supported stream API and, where available, couple Salesforce demand to Orbital downstream capacity. Do not over-fetch just because the gRPC stream can buffer messages.

## 6.9 Keepalive

Salesforce may return an empty response as a keepalive.

```text
Keepalive response
  → do not emit a business event
  → update lifecycle/health telemetry
  → keep the subscription active
```

## 6.10 State machine

Use a state machine with explicit legal transitions:

```text
NEW
AUTHENTICATING
CONNECTED
SUBSCRIBING
SUBSCRIBED
DEGRADED
RECONNECTING
STOPPING
STOPPED
FAILED
```

Typical paths:

```text
NEW → AUTHENTICATING → CONNECTED → SUBSCRIBING → SUBSCRIBED

SUBSCRIBED → RECONNECTING → AUTHENTICATING → CONNECTED → SUBSCRIBING → SUBSCRIBED

SUBSCRIBED → STOPPING → STOPPED

any active state → FAILED
```

No demand request, event emission, or reconnect scheduling is allowed after `STOPPING`.

## 6.11 Reconnect and token refresh

Handle failures deliberately:

```text
UNAUTHENTICATED
  → refresh or reacquire Salesforce session
  → rebuild/reopen stream
  → resume according to configured replay behavior

UNAVAILABLE / transient transport failure
  → exponential backoff with jitter
  → reconnect

INVALID_ARGUMENT caused by invalid replay
  → apply InvalidReplayPolicy

PERMISSION_DENIED / invalid topic / invalid configuration
  → terminal typed failure; do not retry forever
```

Initial backoff profile:

```text
1 second → 2 → 4 → 8 → 16 → 30 seconds maximum
```

Use jitter. Make attempt limits and max delay configurable.

---

# 7. Publish: Orbital to Salesforce Event Bus

## 7.1 v0.1 publish mode

Use Salesforce unary `Publish()` in v0.1.

```java
public record PublishRequest(
    String topic,
    Map<String, Object> payload,
    String correlationKey
) {}
```

If no correlation key is supplied, generate a UUID. Correlation keys are tracing/publish-result correlation data; they are not substitutes for a Salesforce event identity.

For v0.1, a returned receipt means the single publish succeeded.

```java
public record PublishReceipt(
    String topic,
    String correlationKey,
    byte[] replayId,
    String schemaId,
    String rpcId
) {}
```

Failure behavior:

```text
successful Salesforce publish
        ↓
PublishReceipt

Salesforce publish error
        ↓
PublishException
```

Do not return a "successful" receipt containing a failure list for the unary single-event API.

## 7.2 Publish flow

```text
TypedInstance or caller payload
        ↓
GetTopic
        ↓
validate canPublish
        ↓
obtain schema ID
        ↓
schema cache / GetSchema
        ↓
Avro encode
        ↓
ProducerEvent
        ↓
Publish RPC
        ↓
validate publish result
        ↓
PublishReceipt
```

## 7.3 Future publishing

Do not implement in v0.1:

```text
PublishStream
batch publishing
batch Taxi operation
```

Do not prematurely expose a stable batch API. Add it only after implementation, tests, documented error semantics, and real-org validation.

---

# 8. OrbitalHQ Adapter Design

## 8.1 Orbital adapter boundaries

The adapter is intentionally thin:

```text
Owns
  - Taxi annotations
  - connection config parsing and validation
  - OperationInvoker integration
  - TypedInstance mapping
  - streaming/cancellation adaptation
  - observability mapping

Does not own
  - Salesforce gRPC protocol implementation
  - Avro protocol decoding/encoding details
  - OAuth HTTP mechanics
  - downstream destination implementations
```

All `com.orbitalhq` and Taxi imports stay in `orbital-salesforce-connector`. The core module remains usable without OrbitalHQ.

## 8.2 Taxi annotations

Use separate annotations for source and sink behavior.

```taxi
namespace com.orbitalhq.salesforce

annotation SalesforcePubSubService {
   connectionName : String
}

enum SalesforceReplayPreset {
   latest,
   earliest
}

annotation SalesforceSubscribe {
   topic : String
   replay : SalesforceReplayPreset = latest
   consumerName : String?
}

annotation SalesforcePublish {
   topic : String
}

enum SalesforceEventMetadataType {
   EventId,
   ReplayId,
   Topic,
   SchemaId,
   ReceivedAt
}

annotation SalesforceEventMetadata {
   value : SalesforceEventMetadataType
}
```

Do not use one overloaded `@SalesforcePubSubOperation` annotation for both subscribing and publishing.

## 8.3 Taxi validation

Validate connector declarations as early as the Orbital/TAXI extension model permits.

```text
@SalesforceSubscribe
  - Allowed only on a read operation.
  - Must return Stream<T>.
  - Requires a nonblank Salesforce Platform Event topic.
  - Uses an allowed replay preset.

@SalesforcePublish
  - Allowed only on a write operation.
  - Requires exactly one payload parameter in v0.1.
  - Requires a nonblank Salesforce Platform Event topic.

@SalesforcePubSubService
  - Requires a nonblank named connection.

@SalesforceEventMetadata
  - Must be applied only to compatible output fields.
```

Examples of expected failures:

```text
Salesforce subscribe operation `SalesforceEvents::messages` must return Stream<T>;
found `ManualMessageEvent`.

Salesforce publish operation `SalesforceEvents::publish` must be a write operation.
```

## 8.4 Semantic Taxi modeling

Define reusable domain types in domain namespaces and Salesforce physical event models separately.

```taxi
namespace acme.orders

type OrderId inherits String
type CustomerId inherits String
type OrderStatus inherits String
```

```taxi
namespace acme.salesforce.events

closed model OrderStatusChangedEvent {
   OrderId__c : acme.orders.OrderId
   CustomerId__c : acme.orders.CustomerId
   Status__c : acme.orders.OrderStatus
}
```

This supports semantic composition with sources using different field names but the same business meaning.

Use `closed model` by default for event contracts:

```text
- Ignore unused extra Salesforce source fields by default.
- Do not silently accept absence of a required declared Taxi field.
- Map Salesforce optional fields to nullable Taxi fields.
- Keep raw source payload diagnostics opt-in only.
```

## 8.5 Subscribe example

```taxi
import com.orbitalhq.salesforce.SalesforcePubSubService
import com.orbitalhq.salesforce.SalesforceSubscribe

namespace demo.salesforce

type ManualEventPayload inherits String
type SalesforceEventId inherits String
type SalesforceReplayId inherits String

closed model ManualMessageEvent {
   Payload__c : ManualEventPayload

   @SalesforceEventMetadata(EventId)
   eventId : SalesforceEventId?

   @SalesforceEventMetadata(ReplayId)
   replayId : SalesforceReplayId?
}

@SalesforcePubSubService(connectionName = "salesforce-dev")
service SalesforceEvents {

   @SalesforceSubscribe(
      topic = "/event/Manual_Message__e",
      replay = latest,
      consumerName = "manual-message-consumer"
   )
   operation messages(): Stream<ManualMessageEvent>
}
```

## 8.6 Publish example

```taxi
import com.orbitalhq.salesforce.SalesforcePubSubService
import com.orbitalhq.salesforce.SalesforcePublish

namespace demo.salesforce

type ManualEventPayload inherits String
type SalesforceReplayId inherits String
type SalesforceSchemaId inherits String
type SalesforceCorrelationKey inherits String

closed model ManualMessageEvent {
   Payload__c : ManualEventPayload
}

closed model SalesforcePublishReceipt {
   correlationKey : SalesforceCorrelationKey
   replayId : SalesforceReplayId?
   schemaId : SalesforceSchemaId?
}

@SalesforcePubSubService(connectionName = "salesforce-dev")
service SalesforceEvents {

   @SalesforcePublish(topic = "/event/Manual_Message__e")
   write operation publish(
      ManualMessageEvent
   ): SalesforcePublishReceipt
}
```

## 8.7 TypedInstance mapping

Subscribe path:

```text
Salesforce Avro payload
  → Map<String, Object>
  → SalesforceEvent
  → declared Taxi event type
  → TypedInstance
  → Stream<T>
```

Publish path:

```text
TypedInstance
  → Map<String, Object>
  → Avro encode
  → Salesforce Publish
  → PublishReceipt
  → TypedInstance
```

Mapping requirements:

```text
- Preserve nullability.
- Map nested structures only when supported and tested.
- Ignore source fields not present in a closed Taxi model.
- Fail clearly if a required Taxi field cannot be produced.
- Map transport metadata only through explicit @SalesforceEventMetadata fields.
- Base64 encode replay ID at the Taxi boundary.
- Validate outgoing Taxi values before attempting remote publish.
```

## 8.8 Cancellation and stream lifecycle

On Orbital query cancellation, timeout, or normal stream termination:

```text
Orbital stream ends
  → stop requesting Salesforce demand
  → cancel gRPC request stream
  → cancel reconnect tasks
  → release local in-flight buffers
  → close subscription resources
  → preserve any already durable replay state
```

No orphan subscription may remain after the corresponding Orbital stream is no longer active.

## 8.9 Connection configuration

Keep all secrets outside Taxi.

```hocon
salesforcePubSub {
  salesforce-dev {
    connectionName = "salesforce-dev"

    auth {
      type = "clientCredentials"
      loginUrl = ${SF_LOGIN_URL}
      clientId = ${SF_CLIENT_ID}
      clientSecret = ${SF_CLIENT_SECRET}
    }

    endpoint {
      host = "api.pubsub.salesforce.com"
      port = 7443
    }

    subscription {
      initialRequestCount = 10
      refillThreshold = 2
      refillCount = 10
      maxInFlightEvents = 20
      invalidReplayPolicy = "ERROR"
    }

    retry {
      initialBackoff = 1s
      maxBackoff = 30s
      maxConsecutiveFailures = 20
      jitter = 0.20
    }

    grpc {
      keepAliveTime = 60s
      keepAliveTimeout = 20s
      keepAliveWithoutCalls = true
    }
  }
}
```

Environment variables:

```text
SF_LOGIN_URL
SF_CLIENT_ID
SF_CLIENT_SECRET
```

Startup validation may verify that named configuration and required secret references resolve, but it must not eagerly subscribe to every Taxi-declared source operation. Subscribe when Orbital starts the corresponding stream/query.

## 8.10 OperationInvoker requirements

The Orbital adapter must:

```text
- Recognize @SalesforcePubSubService.
- Recognize @SalesforceSubscribe.
- Recognize @SalesforcePublish.
- Route read operations to the Salesforce subscription path.
- Route write operations to the Salesforce publisher.
- Return the Orbital-compatible streaming/result types required by the supported version.
- Disable or avoid inappropriate response caching for event streams.
- Preserve cancellation and failure propagation.
```

Conceptual shape:

```kotlin
class SalesforcePubSubInvoker(
   private val streamManager: SalesforceStreamManager,
   private val publisher: SalesforcePublisher
) : OperationInvoker {
   // Implement against the connector SPI of the supported Orbital release.
}
```

Follow the behavior of current Orbital streaming connectors conceptually, but do not copy their code verbatim.

---

# 9. Observability, Logging, and Health

## 9.1 Telemetry SPI

The core supplies an optional telemetry SPI; the Orbital adapter bridges it to Orbital tracing and metrics.

```java
public interface SalesforcePubSubTelemetry {

    void connected(String connectionName);

    void subscribed(
        String connectionName,
        String topic,
        String consumerName
    );

    void eventReceived(String topic, String eventId);

    void reconnecting(String topic, Throwable cause);

    void published(String topic, String correlationKey);
}
```

Provide a no-op default implementation.

## 9.2 Structured log context

Include, where appropriate:

```text
connectionName
topic
consumerName
serviceName
operationName
Orbital query/request ID where available
schemaId
retry attempt
exception category
replayIdHash
```

Never include:

```text
access token
client secret
refresh token
password
Authorization/gRPC auth metadata
full event payload by default
customer IDs or PII as metric labels
```

Use a short hash rather than the full replay ID in normal logs.

Suggested levels:

```text
INFO   connection, subscription, shutdown, and publish lifecycle
DEBUG  schema cache, flow control, request credit, and keepalive details
WARN   transient failures and retries
ERROR  terminal configuration, authorization, or protocol failures
```

## 9.3 Metrics

Always available:

```text
salesforce_pubsub_connection_state
salesforce_pubsub_subscription_state
salesforce_pubsub_events_received_total
salesforce_pubsub_events_emitted_total
salesforce_pubsub_decode_failures_total
salesforce_pubsub_publish_success_total
salesforce_pubsub_publish_failure_total
salesforce_pubsub_reconnects_total
salesforce_pubsub_schema_cache_hits_total
salesforce_pubsub_schema_cache_misses_total
salesforce_pubsub_in_flight_events
```

Expose only when the selected checkpoint/acknowledgement implementation truly supports these semantics:

```text
salesforce_pubsub_checkpoint_age_seconds
salesforce_pubsub_event_processing_duration
```

Use low-cardinality labels only:

```text
connectionName
topic
outcome
exceptionType
```

## 9.4 Connector health

Do not use raw gRPC channel status as the complete health state. Expose application-level states:

```text
STARTING
AUTHENTICATING
CONNECTED
SUBSCRIBED
RECONNECTING
DEGRADED
FAILED
STOPPED
```

Examples:

| Condition | Health |
|---|---|
| Acquiring token/session | `AUTHENTICATING` |
| gRPC ready but no query stream currently active | `CONNECTED` |
| Active Salesforce Platform Event stream | `SUBSCRIBED` |
| Temporary network error with retry pending | `RECONNECTING` |
| Repeated decode failures with stream still running | `DEGRADED` |
| Invalid credentials, permission, or configuration | `FAILED` |
| Gracefully cancelled/closed stream | `STOPPED` |

Raw gRPC state may be included as diagnostic detail, not as the only user-facing health signal.

---

# 10. Testing Strategy

## 10.1 Test hierarchy

```text
Fast pull-request checks
  - formatting and lint
  - unit tests
  - in-process fake gRPC tests
  - Orbital adapter tests
  - static analysis and packaging checks

Protected branch / scheduled checks
  - all fast checks
  - local Orbital smoke test
  - real Salesforce org smoke/integration tests
  - release packaging/signing verification
```

Do not run real Salesforce tests on every pull request unless the organization can guarantee stable, isolated credentials and test org availability.

## 10.2 Core unit tests

### Authentication

```text
- Client credentials token acquisition succeeds.
- Caller-provided session is accepted.
- Missing access token fails.
- Missing instance URL fails.
- Refresh/reacquisition behavior works.
- Concurrent refresh is safe.
- Secrets/tokens are redacted from logs and exceptions.
- UNAUTHENTICATED maps to AuthenticationException.
- PERMISSION_DENIED maps to AuthorizationException.
```

### gRPC metadata

```text
- Adds accesstoken.
- Adds instanceurl.
- Adds tenantid.
- Uses refreshed values for subsequent calls.
- Does not leak metadata into logs.
```

### Topic and schema

```text
- Valid topic resolves.
- Unknown topic maps to TopicNotFoundException.
- Cannot-subscribe and cannot-publish cases fail appropriately.
- Schema cache miss calls GetSchema.
- Schema cache hit avoids unnecessary GetSchema.
- New schema ID triggers a new fetch.
- Cache is bounded and eviction behavior is tested.
- Schema lookup timeouts/failures are typed and retried only when appropriate.
```

### Avro decoding

```text
- Strings, booleans, integer/long, floating point, dates, timestamps, nulls.
- Nullable unions.
- Arrays and nested records.
- UTF8 values normalize to String.
- Malformed payload creates EventDecodeException.
- Incompatible schema/payload creates EventDecodeException.
- Exceptions carry safe schema/topic/replay context without raw payload leakage.
```

### Avro encoding

```text
- Valid Platform Event payload encodes.
- Missing required field fails before Publish RPC.
- Wrong type fails before Publish RPC.
- Nullable fields encode correctly.
- Nested structures encode correctly if supported.
```

### Replay

```text
- Explicit ReplayId takes precedence.
- Existing ReplayStore checkpoint takes precedence over replay preset.
- Replay preset is used only when there is no explicit replay ID and no stored checkpoint.
- Latest start.
- Earliest start.
- Custom replay start through core API.
- CLI accepts Base64 replay ID.
- Replay bytes remain opaque.
- Base64 conversion occurs only at a textual boundary.
- Invalid replay policy ERROR fails without silent reset.
- Explicit EARLIEST/LATEST fallback behaves as configured.
- Replay store key includes connection, topic, and consumer name.
```

### Flow control

```text
- Initial demand is requested.
- Demand refills at configured threshold.
- maxInFlightEvents is never exceeded.
- Invalid flow-control settings fail validation.
- No additional request after cancel, stop, or terminal failure.
- Keepalive does not consume an event slot or emit business event.
```

### Retry/reconnect

```text
- UNAVAILABLE uses exponential backoff with jitter.
- UNAUTHENTICATED triggers configured refresh/re-authentication.
- Permission/configuration/topic errors do not retry forever.
- Scheduled reconnect is cancelled by explicit close.
- Reconnection starts from the intended replay strategy.
```

### Publish

```text
- Unary publish succeeds.
- Caller correlation key is returned/mapped.
- Missing correlation key is generated.
- Salesforce publish result error raises PublishException.
- A returned PublishReceipt always represents success.
- Payload and secrets are redacted from ordinary error output.
```

## 10.3 State-machine tests

Test every legal and illegal state transition:

```text
NEW → AUTHENTICATING → CONNECTED → SUBSCRIBING → SUBSCRIBED
SUBSCRIBED → RECONNECTING → AUTHENTICATING → CONNECTED → SUBSCRIBING → SUBSCRIBED
SUBSCRIBED → STOPPING → STOPPED
active state → FAILED
```

Assert that demand requests, event emission, replay store writes, and reconnect scheduling stop after `STOPPING`.

## 10.4 Fake gRPC tests

Use an in-process fake Salesforce Pub/Sub gRPC server based on generated proto stubs.

Test:

```text
- Required metadata reaches the server.
- GetTopic runs before subscribe and publish.
- GetSchema runs once per uncached schema ID.
- ConsumerEvent Avro payloads decode correctly.
- Keepalive is ignored as business data.
- Demand/refill request numbers are correct.
- Event order is retained for a single subscription.
- Stream interruption triggers reconnect behavior.
- Token invalidation triggers refresh/reconnection.
- Custom replay is sent correctly.
- Invalid replay follows policy.
- Cancellation closes client stream and stops demand.
- Publish emits expected Avro ProducerEvent.
- Publish error raises PublishException.
```

## 10.5 Orbital adapter tests

Test separately from generic Salesforce core behavior.

### Taxi declaration validation

```text
- @SalesforceSubscribe is recognized.
- @SalesforcePublish is recognized.
- Subscribe requires Stream<T>.
- Publish requires a write operation.
- Invalid/missing topic is rejected.
- Missing connection name is rejected.
- Invalid metadata annotations are rejected.
- Raw custom replay IDs are not embedded in Taxi annotations.
```

### Mapping

```text
- Decoded Map<String,Object> maps to TypedInstance.
- Nullable source values map to nullable Taxi properties.
- Missing required Taxi field produces an actionable mapping error.
- Extra unused Salesforce fields are ignored for closed models.
- Metadata maps only to explicit metadata fields.
- Replay ID is Base64 encoded at Taxi boundary.
- TypedInstance publishes to the expected map/payload.
- Publish receipt maps to the Taxi result type.
```

### Stream lifecycle

```text
- Orbital stream cancellation cancels the gRPC subscription.
- Cancellation prevents new demand/refill requests.
- No thread/coroutine/channel/resource leak occurs after cancellation.
- Normal cancellation becomes STOPPED, not FAILED.
- Flow/backpressure behavior follows the actual supported Orbital connector SPI.
```

### Checkpoint behavior

Checkpoint tests must reflect the real Orbital acknowledgement behavior discovered during implementation. Do not fabricate a downstream-completion guarantee if the SPI does not expose one.

## 10.6 Real Salesforce integration tests

Use a dedicated non-production Salesforce Developer Edition, scratch org, or sandbox.

Test:

```text
- Client credentials authentication where the target org supports/configures it.
- Caller-provided session mode.
- GetTopic for /event/Manual_Message__e.
- GetSchema.
- Subscribe to Manual_Message__e.
- Publish Manual_Message__e.
- LATEST and EARLIEST initial-start policies.
- CUSTOM replay through the core API / CLI where test retention permits.
- Existing replay-store checkpoint resumes ahead of replay preset.
- Schema change/new schema ID behavior.
- Reconnect and session refresh where practical.
- Orbital stream receives the expected event type.
- Orbital write operation publishes the expected event.
```

Use isolated test event names and credentials. Do not use production tenant secrets in CI.

## 10.7 v0.1 acceptance tests

```text
Subscribe:
Salesforce publishes Manual_Message__e
  → Orbital receives Stream<ManualMessageEvent>
  → Payload__c maps correctly.

Publish:
Orbital invokes @SalesforcePublish
  → Salesforce receives Manual_Message__e
  → Payload__c is correct.

Replay:
LATEST and EARLIEST initial-start policies work as documented.
Custom replay works through the core API / CLI.
Existing ReplayStore checkpoint overrides initial replay preset.
Invalid replay fails under ERROR policy.

Reconnect:
Drop transient gRPC connection
  → connector reconnects
  → subscription resumes according to the documented replay strategy.

Auth refresh:
Session becomes invalid
  → connector reacquires/refreshes session
  → opens a usable subscription.

Schema evolution:
New schema ID
  → schema fetched
  → compatible event decoding continues.

Cancellation:
Orbital stream cancellation
  → Salesforce gRPC subscription closes
  → no new demand is sent.
```

---

# 11. Code Quality, Security, and CI

## 11.1 Required quality tools

| Area | Tooling | Required rule |
|---|---|---|
| Java formatting | Spotless with Google Java Format or Palantir Java Format | `spotless:check` passes |
| Kotlin formatting | ktlint through Spotless or dedicated plugin | Formatting check passes |
| Kotlin static analysis | Detekt | No new production-code findings above agreed threshold |
| Java static analysis | Error Prone and/or SpotBugs | Fail high-confidence correctness issues |
| Tests and coverage | JUnit 5, AssertJ, Mockito as needed, JaCoCo | Coverage thresholds and critical-package tests pass |
| Dependency security | Dependabot plus OWASP Dependency-Check or equivalent | High/critical findings reviewed and blocked per policy |
| Secret scanning | Gitleaks | Mandatory on pull requests and scheduled scans |
| Code security | CodeQL | Run on pull requests and scheduled scans |
| License review | License Maven Plugin / equivalent | Third-party licenses checked |
| API compatibility | Revapi or japicmp after API stabilizes | Detect accidental breaking public API changes |
| Documentation | Markdown/link validation | Documentation links and snippets remain valid |

Suggested quality targets:

```text
Core module:
  line coverage >= 80%
  branch coverage >= 70%

Authentication, replay, Avro, and subscription state machine:
  line coverage >= 90%
  branch coverage >= 85%
```

Coverage does not replace behavior testing. The highest-value tests are state-machine, replay, cancellation, flow-control, and real protocol tests.

## 11.2 CI pipeline

### Pull request pipeline

```text
1. Checkout
2. Gitleaks
3. Spotless / ktlint
4. Compile
5. Unit tests
6. In-process fake gRPC tests
7. Orbital adapter tests
8. JaCoCo report and thresholds
9. Detekt / Error Prone / SpotBugs
10. Dependency and license scanning
11. CodeQL where configured
12. Maven package build
13. Artifact smoke test
```

### Main branch pipeline

```text
Everything from pull request checks
+
14. Build container if a container distribution is provided
15. Start a reproducible local Orbital smoke environment
16. Load sample Taxi project
17. Run fake-Salesforce subscribe/publish connector smoke test
18. Publish snapshot artifacts if desired
```

### Scheduled/protected release pipeline

```text
Everything from main checks
+
19. Real Salesforce non-production integration suite
20. Release artifact signing verification
21. SBOM generation
22. Provenance/attestation generation if available
23. Publish Maven Central artifacts
24. Create GitHub release and changelog
25. Publish release notes, checksums, SBOM, and signed artifacts
```

## 11.3 Security and release hygiene

Include:

```text
Apache-2.0 LICENSE
NOTICE
CONTRIBUTING.md
SECURITY.md
CODE_OF_CONDUCT.md
CHANGELOG.md
issue and PR templates
architecture decision records in docs/adr/
Dependabot configuration
CodeQL configuration
Gitleaks configuration
```

Release requirements:

```text
- Signed release artifacts.
- Signed Git tag where organizational policy supports it.
- Source JAR and Javadoc JAR.
- SBOM attached to release.
- No secrets in fixtures, logs, docs, examples, or CI output.
- Dedicated test-org credentials with documented rotation procedure.
```

---

# 12. Documentation Requirements

The README must include:

```text
Overview
Architecture
Features
Scope and non-goals
Compatibility
Installation
Maven coordinates
OrbitalHQ compatibility matrix
Salesforce prerequisites
Authentication setup
Connection configuration
Subscribe example
Publish example
Replay semantics
Replay-start precedence
Custom replay behavior
Flow control
Cancellation behavior
Error handling
Observability
Testing
Development
Security
Known limitations
Troubleshooting
License
Community-project disclaimer
```

Include this disclaimer:

```text
This is a community project and is not affiliated with or endorsed by
Salesforce or OrbitalHQ.
```

## 12.1 Orbital-first quick start

The quick start must let an Orbital user:

```text
1. Add the connector artifact.
2. Configure a named Salesforce connection in connections.conf.
3. Supply secrets through environment variables or a secret manager.
4. Add Taxi scalar/event definitions.
5. Declare a @SalesforceSubscribe Stream<T> operation.
6. Start an Orbital stream.
7. Declare a @SalesforcePublish write operation.
8. Publish a test Platform Event.
9. Understand replay/cancellation/delivery limitations.
```

## 12.2 Operational statements

Document clearly:

```text
- This project consumes and publishes Salesforce custom Platform Events.
- It uses Salesforce Pub/Sub API, not CometD, in v0.1.
- Replay IDs are opaque stream positions, not business identifiers.
- Replay preset is only the initial-start policy when no explicit replay ID or stored checkpoint exists.
- Invalid replay defaults to ERROR to prevent silent loss/reset.
- Custom replay is exposed through the core API and CLI, not embedded in Taxi annotations.
- Event subscriptions consume Salesforce delivery capacity; users should start them deliberately.
- The connector does not claim exactly-once delivery.
- Replay checkpoint semantics depend on the supported Orbital connector SPI and are documented precisely for each release.
```

## 12.3 Troubleshooting table

| Symptom | Likely cause | Action |
|---|---|---|
| `AuthenticationException` | OAuth/session configuration invalid or expired | Verify login URL, external/connected app settings, client credentials, secret injection, and integration principal |
| `AuthorizationException` | Principal lacks API/topic permission | Verify Salesforce API access and Platform Event permissions |
| `TopicNotFoundException` | Topic invalid or event not deployed | Verify `/event/<EventName>__e` and Salesforce metadata |
| `InvalidReplayException` | Replay position expired or invalid | Investigate checkpoint age; choose recovery policy explicitly |
| `EventDecodeException` | Schema and payload mismatch | Inspect schema ID, event schema evolution, and Taxi field mapping |
| Persistent `RECONNECTING` | Network/TLS/proxy/gRPC egress issue | Validate outbound HTTP/2/TLS connectivity to the configured Pub/Sub endpoint |
| Publish failure | Schema mismatch, topic permissions, or field validation | Validate `GetTopic`, schema, outgoing Taxi values, and Salesforce publish result |

---

# 13. Implementation Order

Do not start with the Orbital adapter. Prove the Salesforce protocol independently first.

```text
1. Create Maven multi-module repository.
2. Add formatting, linting, test, and security baseline.
3. Add Salesforce proto generation.
4. Implement typed configuration models.
5. Implement authentication SPI.
6. Implement client-credentials provider.
7. Implement caller-supplied session provider.
8. Implement gRPC metadata credentials.
9. Implement GetTopic.
10. Implement GetSchema.
11. Implement bounded schema cache.
12. Implement Avro decoder.
13. Implement Avro encoder.
14. Implement neutral SalesforceEvent model.
15. Implement subscription start/replay models.
16. Implement replay-start precedence.
17. Implement subscription state machine.
18. Implement bounded flow control.
19. Implement Subscribe RPC.
20. Implement replay-store SPI and in-memory store.
21. Implement invalid-replay behavior.
22. Implement reconnect/backoff.
23. Implement token refresh/re-authentication.
24. Implement typed errors and lifecycle telemetry.
25. Implement unary Publish RPC.
26. Implement CLI get-topic/get-schema/subscribe/publish commands.
27. Implement CLI custom replay via Base64 replay ID.
28. Validate CLI against a real non-production Salesforce org.
29. Add core unit tests.
30. Add in-process fake gRPC tests.
31. Inspect supported Orbital Kafka/SQS source semantics and document actual acknowledgement/checkpoint capabilities.
32. Implement Taxi annotations and declaration validation.
33. Implement connection configuration mapping.
34. Implement OperationInvoker against the supported Orbital connector SPI.
35. Implement Stream<TypedInstance> adaptation.
36. Implement TypedInstance subscribe/publish mapping.
37. Implement protocol metadata mapping.
38. Implement cancellation propagation.
39. Add Orbital adapter tests.
40. Run Salesforce subscribe through Orbital.
41. Run Salesforce publish through Orbital.
42. Add local Orbital smoke environment.
43. Complete README, operations docs, and troubleshooting.
44. Publish an explicitly documented 0.1.0 release.
```

## 13.1 Mandatory stop condition

Before implementing or advertising durable replay checkpointing tied to downstream processing, inspect the supported Orbital version’s source-connector behavior and report:

```text
- Kafka commit behavior.
- SQS acknowledgement/delete behavior.
- Delivery-success/failure callback availability.
- OperationInvoker interaction with source stream lifecycle.
- Cancellation behavior.
- What checkpoint semantics are actually implementable.
```

Do not create an acknowledgment API solely because it is architecturally appealing if Orbital cannot invoke it at the true downstream completion point.

---

# 14. v0.1 Definition of Done

```text
[ ] Core library compiles and is independently usable.
[ ] CLI compiles and uses the core library, not duplicate protocol code.
[ ] Orbital connector compiles against a documented released Orbital version.
[ ] Client-credentials auth works in a validated non-production Salesforce org where configured.
[ ] Caller-provided session mode works.
[ ] Required gRPC metadata is attached correctly.
[ ] GetTopic and GetSchema work.
[ ] Schema cache is bounded and tested.
[ ] Avro decoding works for supported types.
[ ] Avro encoding works for supported Platform Event payloads.
[ ] Subscribe works for custom Platform Events.
[ ] Unary publish works for custom Platform Events.
[ ] LATEST and EARLIEST initial-start policies work as documented.
[ ] Explicit custom replay works through the core API and CLI.
[ ] Existing ReplayStore checkpoint overrides initial replay preset.
[ ] Replay IDs remain opaque internally.
[ ] Invalid replay default ERROR works.
[ ] Flow control is bounded.
[ ] Reconnect and token refresh work.
[ ] Keepalive handling works.
[ ] Lifecycle events and health states work.
[ ] Typed error model works.
[ ] Graceful close works.
[ ] @SalesforceSubscribe validates and returns Stream<T>.
[ ] @SalesforcePublish validates and invokes a write operation.
[ ] Salesforce payload maps to TypedInstance.
[ ] Protocol metadata maps only when explicitly requested.
[ ] Orbital stream cancellation closes Salesforce resources.
[ ] Checkpoint and delivery semantics are documented accurately for the supported Orbital version.
[ ] Unit, fake gRPC, and Orbital adapter tests pass.
[ ] Real Salesforce subscribe and publish smoke tests pass.
[ ] Formatting, linting, static analysis, secret scanning, and security checks pass.
[ ] Maven package smoke test passes.
[ ] README/configuration/replay/troubleshooting documentation is complete.
[ ] No credentials or sensitive payload fixtures are committed.
[ ] Maven Central release artifacts are signed and publishable.
```

---

# 15. Final Recommendation

Build a focused two-layer project:

```text
Reusable Salesforce Pub/Sub API core
        +
Thin OrbitalHQ connector adapter
```

The v0.1 project must do two things well and only two things:

```text
1. Salesforce custom Platform Event → OrbitalHQ Stream<T>
2. OrbitalHQ write operation → Salesforce custom Platform Event
```

Keep transport, authentication, schema lookup, Avro handling, replay, flow control, reconnect, and publishing inside a reusable Salesforce core library. Keep Taxi annotations, `TypedInstance` mapping, `Stream<T>`, `OperationInvoker`, cancellation, and Orbital observability inside the Orbital adapter.

The most important implementation disciplines are:

```text
- Replay-start precedence must be deterministic.
- Replay preset is only an initial-start policy.
- Custom replay belongs in the core API/CLI, not Taxi source.
- A unary PublishReceipt represents success; failures raise PublishException.
- Checkpoint-related metrics are exposed only when the real Orbital acknowledgement model supports them.
- Reliability claims must match actual behavior verified against the supported Orbital connector SPI.
```

Do not expand the project into downstream persistence or workflow orchestration, and do not claim exactly-once delivery. A narrow, validated, well-tested connector will be much more valuable to OrbitalHQ users than a broad integration framework.
