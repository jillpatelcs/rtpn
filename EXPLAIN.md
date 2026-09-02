# RTPN Payment Rail Explained

## 1. What This Project Is

RTPN is a real-time payment rail simulator inspired by systems such as Canada's Real-Time Rail and Interac payment infrastructure.

It models the complete lifecycle of an interbank payment:

- Financial institutions create ISO 20022 `pacs.008` payment messages.
- Kafka transports messages to a central clearing hub.
- The hub validates, risk-checks, and settles payments.
- PostgreSQL stores settlement accounts and the authoritative accounting records.
- MongoDB stores an append-oriented payment audit trail.
- An outbox poller publishes cleared or rejected results back to Kafka.
- A reconciliation job proves that account balances match the audit history.
- A separate AI copilot provides read-only operational queries.

The project is a simulator, not a production payment system. It supports one currency, CAD, uses gross settlement, has a single Kafka broker, and does not implement authentication.

## 2. Architecture

```text
Bank simulator or REST client
             |
             v
Kafka: payments.inbound
             |
             v
Spring Boot clearing hub
  1. Parse payment
  2. Validate
  3. Deduplicate
  4. Risk check
  5. Settle funds
             |
             +--> PostgreSQL
             |      settlement_accounts
             |      ledger_entries
             |      processed_messages
             |      outbox_events
             |
             +--> MongoDB
             |      payment_audit
             |
             v
Outbox poller
             |
             +--> payments.cleared
             +--> payments.rejected

MongoDB audit + hub REST API
             |
             +--> reconciliation.py
             +--> FastAPI/Claude operations copilot
```

The Java application starts at [HubApplication.java](hub/src/main/java/ca/rtpn/hub/HubApplication.java). Docker service definitions are in [docker-compose.yml](docker-compose.yml).

## 3. Technology Stack

### Clearing hub

- Java 17
- Spring Boot 3.3.5
- Spring Web for REST APIs
- Spring Data JPA for PostgreSQL access
- Spring Data MongoDB for audit records
- Spring Kafka for Kafka integration
- Maven for builds and dependency management
- JUnit and Mockito for tests

The dependencies are defined in [pom.xml](hub/pom.xml).

### Infrastructure

- PostgreSQL 16
- MongoDB 7
- Apache Kafka 3.7 in single-node KRaft mode
- Docker Compose

### Python services

- Python 3.10+
- `kafka-python` for the bank simulator
- `pymongo` and `requests` for reconciliation
- FastAPI and Uvicorn for the copilot API
- Anthropic Python SDK for Claude tool use
- pytest for Python tests

## 4. End-to-End Payment Workflow

### Step 1: Generate a payment

[bank_simulator.py](participants/bank_simulator.py) randomly chooses two different participants, generates an amount, and builds a minimal ISO 20022 `pacs.008.001.08` XML document.

The simulator can deliberately redeliver messages. For example, with `--duplicate-pct 5`, approximately five percent of the messages reuse an existing `messageId`.

### Step 2: Publish to Kafka

The simulator publishes XML to `payments.inbound`.

The Kafka key is the debtor participant. This helps preserve ordering for messages from the same participant when multiple partitions are used.

### Step 3: Parse the XML

[PaymentInboundConsumer.java](hub/src/main/java/ca/rtpn/hub/kafka/PaymentInboundConsumer.java) receives the XML and extracts:

- `MsgId`
- `EndToEndId`
- Debtor participant
- Creditor participant
- Interbank settlement amount
- Currency

The parser uses XPath expressions based on `local-name()`, so it does not depend on a particular XML namespace prefix.

Malformed XML is logged and discarded. Because it may not contain a valid message ID, it cannot currently be recorded as a normal rejection.

### Step 4: Run the clearing pipeline

[ClearingService.java](hub/src/main/java/ca/rtpn/hub/service/ClearingService.java) orchestrates the pipeline:

```text
validate -> deduplicate -> risk check -> settle -> audit -> publish outcome
```

The REST API uses the same pipeline, so REST and Kafka submissions have the same business behavior.

### Step 5: Validate

[ValidationService.java](hub/src/main/java/ca/rtpn/hub/service/ValidationService.java) rejects payments when:

- `messageId` is missing
- Debtor or creditor is missing
- Debtor and creditor are the same participant
- Amount is null, zero, or negative
- Amount has more than two decimal places
- Currency is not CAD

These payments receive `REJECTED_VALIDATION`.

### Step 6: Deduplicate

The hub checks `processed_messages` using the payment's `messageId`.

If the message was already processed, the hub returns `DUPLICATE` and does not debit or credit any account.

The application lookup is only a fast path. The real concurrency guarantee is the PostgreSQL primary key on `processed_messages.message_id`.

### Step 7: Risk check

[RiskService.java](hub/src/main/java/ca/rtpn/hub/service/RiskService.java) currently implements a sliding one-minute velocity rule:

```text
No more than 25 processed payments per debtor in one minute
```

The rule is intentionally simple. A production rail could add sanctions screening, amount limits, fraud scoring, account status checks, and anomaly detection.

One implementation detail worth mentioning in an interview: the current count includes all processed records, including rejected records, and the check is not itself a transactional reservation.

### Step 8: Settle the payment

[SettlementService.java](hub/src/main/java/ca/rtpn/hub/service/SettlementService.java) performs real-time gross settlement inside a PostgreSQL transaction.

For a payment from `ALPHA_BANK` to `BETA_BANK`:

```text
ALPHA_BANK balance -= amount
BETA_BANK  balance += amount
```

The service also creates two ledger entries:

```text
DEBIT  ALPHA_BANK  amount
CREDIT BETA_BANK   amount
```

If the debtor lacks sufficient prefunded liquidity, the payment receives `REJECTED_INSUFFICIENT_FUNDS` and no ledger entries are created.

### Step 9: Store terminal state

The `processed_messages` row stores the message ID, participants, amount, and terminal status. This makes rejected messages idempotent too: retrying the same rejected message does not process it repeatedly.

### Step 10: Audit and publish the result

The hub stores a MongoDB audit record containing the original payment payload, status, reason, and processing timestamp.

It also creates an outbox event. [OutboxPoller.java](hub/src/main/java/ca/rtpn/hub/kafka/OutboxPoller.java) periodically sends unpublished events to either `payments.cleared` or `payments.rejected`.

The poller marks an event as published only after Kafka confirms the send. If the hub crashes before publishing, the event remains pending and can be retried.

## 5. Database Design

The schema is defined in [init-db.sql](scripts/init-db.sql).

### `settlement_accounts`

Stores each participant's prefunded account:

- Participant ID
- Current balance
- Opening balance
- Currency

The current balance is a cached, fast-access value. The ledger is the accounting source of truth.

### `ledger_entries`

Stores append-only debit and credit entries. Every settled payment creates exactly two entries linked by `message_id`.

### `processed_messages`

Stores terminal payment state and provides database-backed idempotency through its primary key.

### `outbox_events`

Stores events waiting to be published to Kafka. Rows remain unpublished until the poller successfully sends them.

### `payment_audit` in MongoDB

Stores the audit history for payments, including rejections. MongoDB is useful here because audit data is document-shaped and queried by status, participant, reason, and time.

## 6. Concurrency and Consistency

### Account locking

Settlement locks both participant account rows using `SELECT ... FOR UPDATE`.

Locks are always acquired in lexicographic participant order, regardless of payment direction. Therefore, concurrent payments such as:

```text
ALPHA_BANK -> BETA_BANK
BETA_BANK  -> ALPHA_BANK
```

do not lock the same rows in opposite orders. This prevents the common deadlock pattern.

### Double-entry accounting

Every settlement has equal debit and credit amounts. Consequently, all participant net positions should sum to zero in this closed system.

### Exactly-once qualification

Settlement is protected against duplicate `messageId` processing by the PostgreSQL primary key and transaction boundaries.

Kafka outcome delivery is more accurately described as at-least-once. If the process crashes after Kafka accepts an event but before the outbox row is marked published, the event can be sent again. Downstream consumers must therefore be idempotent.

## 7. Reconciliation

[reconciliation.py](participants/reconciliation.py) independently replays all settled MongoDB audit records.

For each participant it calculates:

```text
expected delta = credits - debits
actual delta   = current balance - opening balance
```

It then verifies:

```text
expected delta == actual delta
```

It also verifies that the total net position is zero. If either check fails, reconciliation exits with failure.

This catches money leaks, missing postings, incorrect cached balances, and audit/ledger divergence.

## 8. REST API

The REST controller is [HubController.java](hub/src/main/java/ca/rtpn/hub/api/HubController.java).

```text
POST /api/v1/payments
GET  /api/v1/payments/{messageId}
GET  /api/v1/accounts
GET  /api/v1/accounts/{participantId}
```

Example request:

```json
{
  "messageId": "demo-001",
  "endToEndId": "E2E-DEMO",
  "debtorParticipant": "ALPHA_BANK",
  "creditorParticipant": "GAMMA_CU",
  "amount": 125.50,
  "currency": "CAD"
}
```

Submitting the same request again should return `DUPLICATE`.

## 9. AI Operations Copilot

The copilot is a separate FastAPI service. Its files are [app.py](copilot/app.py), [agent.py](copilot/agent.py), and [tools.py](copilot/tools.py).

Its workflow is:

```text
question -> FastAPI /ask -> Claude tool-use loop
                              |
                              +--> MongoDB audit search
                              +--> MongoDB statistics
                              +--> hub REST account lookup
                              |
                              v
                       grounded operational answer
```

It has four read-only tools:

- Get one payment's audit history
- Search recent payments
- Read current settlement account balances
- Get aggregate rail statistics

The copilot cannot create, retry, reverse, or modify payments. This is an important architectural boundary: the AI is outside the money-movement path, while clearing remains deterministic and auditable.

## 10. Running the Project

Requirements:

- Docker and Docker Compose
- Java 17
- Maven
- Python 3.10+

Start infrastructure:

```bash
docker compose up -d postgres mongo kafka
```

Run the hub locally:

```bash
cd hub
mvn spring-boot:run
```

Generate payment traffic:

```bash
cd participants
pip install -r requirements.txt
python bank_simulator.py --count 200 --rate 20 --duplicate-pct 5
```

Inspect account balances:

```bash
curl http://localhost:8080/api/v1/accounts
```

Consume cleared events:

```bash
docker compose exec kafka kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic payments.cleared \
  --from-beginning
```

Run reconciliation:

```bash
python reconciliation.py
```

Run the copilot:

```bash
cd copilot
pip install -r requirements.txt
```

Set `ANTHROPIC_API_KEY`, then run:

```bash
uvicorn app:app --port 8090
```

The demo page is available at `http://localhost:8090`. The API accepts `POST /ask` with a JSON body such as:

```json
{
  "question": "which participant has the lowest settlement balance right now?"
}
```

## 11. Testing

Java tests are located under [hub/src/test](hub/src/test) and cover:

- Validation rules
- Successful settlement
- Insufficient funds
- Unknown participants
- Balanced double-entry posting
- Lock ordering

Python tests cover reconciliation arithmetic and copilot query logic. Important files include [test_reconciliation.py](participants/tests/test_reconciliation.py) and [test_tools.py](copilot/tests/test_tools.py).

Run tests with:

```bash
cd hub
mvn test

cd ../participants
pytest

cd ../copilot
pytest
```

## 12. Honest Limitations

The project intentionally does not implement:

- Multiple currencies or foreign exchange
- Net settlement cycles
- A full fraud engine
- Multi-broker Kafka replication
- Active-active multi-region deployment
- Authentication and authorization
- TLS for local infrastructure
- Full ISO 20022 schema validation
- Dead-letter handling for malformed messages

There is also an important implementation qualification: `ClearingService.finish()` writes the Mongo audit record and outbox event after `SettlementService.settle()` returns. Therefore, PostgreSQL settlement is atomic, but PostgreSQL settlement, Mongo audit persistence, and outbox creation are not currently one cross-database atomic transaction. A crash between those operations could leave the balance settled while audit or outcome data is missing.

## 13. Production Improvements

For a production-quality system, I would add:

- A reliable atomic or recovery-based design for audit and outbox persistence
- Kafka replication and stronger delivery guarantees
- Idempotent downstream outcome consumers
- Dead-letter topics and retry policies
- Full XML schema validation
- XML parser hardening against XXE and malicious input
- Authentication, authorization, TLS, and secret management
- Metrics, tracing, alerts, and operational dashboards
- Concurrency-safe risk reservations
- Stronger sanctions, fraud, and anomaly screening
- Support for currencies, limits, reversals, and participant lifecycle states

## 14. Interview Explanation

Use this summary:

> RTPN is a real-time payment rail simulator. Banks generate ISO 20022 pacs.008 messages and publish them to Kafka. A Spring Boot clearing hub consumes those messages, validates them, checks for duplicates, applies a velocity risk rule, and settles valid payments in PostgreSQL. Settlement locks both participant accounts in deterministic order, debits the sender, credits the receiver, writes balanced double-entry ledger records, and stores a processed-message record for idempotency. MongoDB stores the audit trail, and an outbox poller publishes cleared or rejected outcomes to Kafka. A Python reconciliation job replays settled audit records and compares expected participant movements with live balances. A separate FastAPI/Claude copilot provides read-only operational analysis without being able to modify the payment system.

## 15. Common Interview Questions

### Why PostgreSQL for settlement?

Settlement needs ACID transactions, row locking, constraints, and atomic debit/credit updates. PostgreSQL is the authoritative store for money movement.

### Why MongoDB for audit?

Audit records are append-oriented, document-shaped, and queried by status, participant, reason, and time. They do not need to participate in the core balance transaction.

### How is duplicate settlement prevented?

The `processed_messages.message_id` primary key prevents two concurrent deliveries of the same message from both committing. The application-level existence check is only a performance optimization.

### How are deadlocks prevented?

Both account rows are always locked in the same sorted order, regardless of whether the payment direction is A-to-B or B-to-A.

### How do you prove that money was not created or destroyed?

Each settled payment creates one debit and one equal credit. Reconciliation verifies participant balance deltas and checks that all net positions sum to zero.

### Is the system exactly once?

Settlement is idempotent per `messageId`. Outcome publication is retryable and can be at-least-once, so consumers must deduplicate. The current implementation also has a gap between PostgreSQL settlement and Mongo/outbox persistence that should be acknowledged honestly.

### Why is the AI not allowed to settle payments?

An LLM is probabilistic and non-deterministic. Keeping it read-only and outside the settlement path protects financial correctness, auditability, and operational safety.