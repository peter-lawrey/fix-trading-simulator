# Fix Trading Simulator

- [Overview](#overview)
- [System Architecture](#system-architecture)
- [Project Structure](#project-structure)
- [Features](#features)
- [Running the project](#running-the-project)
- [Images](#images)
- [Help Queries](#help-queries)


# Overview

A trading simulator between a Broker and a Stock Exchange using the [Financial Information eXchange (FIX) Protocol](https://www.fixtrading.org/). It's a study project using [QuickFIX/J](https://www.quickfixj.org/), [Quarkus](https://quarkus.io/), [Angular](https://angular.io/), Docker, Docker Compose and [PostgreSQL](https://www.postgresql.org/).

If you want to participate on this project, just open an issue and we can talk about!

Both Broker and Exchange systems were built with Quarkus on the back-end and Angular on the front-end. 

QuickFIX/J connects the broker and exchange. Each process keeps its book in memory and writes a local append-only journal. The reporting gateway projects the two journals into separate PostgreSQL schemas.

Each Angular front-end communicates with the Quarkus back-end using REST and Websockets.

# System Architecture

The original direct-SQL design is illustrated below. On this branch the journal and reporting gateway separate trading from database access.

![Original JDBC architecture](./documentation/design/fix-trading-simulator-design.png)

# Project Structure

[Broker back-end](./broker-back-end/README.md)

[Broker front-end](./broker-front-end/README.md)

[Exchange back-end](./exchange-back-end/README.md)

[Exchange front-end](./exchange-front-end/README.md)

[Documentation](./documentation/README.md)

# Features

## Orders

You can submit, negotiate, cancel and list your orders.

It's possible to set the Exchange to automatically negotiate the orders.

Every change in the orders are broadcasted using websockets and are imeaditelly updated on the front-end.

## Session

Make logon and logout.

View the session status and storage.

View the session configuration.

List the messages sent from the session.

## Logs

List the FIX events.

List the messages received and sent.


# Running the project

This branch uses QuickFIX/J 2.3.0, a JDK file journal and a separate PostgreSQL reporting process.
The [JDBC branch](https://github.com/peter-lawrey/fix-trading-simulator/tree/sql-optimisations) retains the direct database implementation.
The original Docker Hub images run the original JDBC implementation.

## Start the broker and exchange

Use Maven and JDK 17 in the repository root. Each role owns its own directory for its QuickFIX/J FileStore, directional FIX logs and business journal.

```bash
mvn clean verify
java -jar exchange-back-end/target/quarkus-app/quarkus-run.jar
# In a second terminal
java -jar broker-back-end/target/quarkus-app/quarkus-run.jar
```

The broker listens on HTTP port 8080, the exchange on 8090, and the FIX pair connects on loopback port 9880.
Set `SIM_FIX_HOST`, `SIM_FIX_PORT`, `SIM_SENDER`, `SIM_TARGET` and `SIM_STORE` per process to run another session.
The defaults store each role under `data/<role>`. A fresh JVM reopens the same session and replays its journal.

```bash
curl -H 'Content-Type: application/json' -d '{"side":"1","symbol":"IBM","orderQty":10,"price":23.50}' http://localhost:8080/orders
curl 'http://localhost:8080/orders?order=desc&limit=100'
python3 scripts/check-journal.py
```

Use `ws://localhost:8080/socket/order-commands` to submit the same order JSON and receive a private admission response.
The order can still be awaiting an execution report when admission succeeds. Dashboard notifications come from
`/socket/order?hello=1`. Wait for the subscription acknowledgement before loading a page. An interrupted command
connection can leave its outcome unknown. Reconcile the order before resubmitting.

## Project the journal to PostgreSQL

Start with empty broker and exchange schemas created using `init-broker.sql` and `init-exchange.sql`.
Configure `SIM_DB_URL` (for example `jdbc:postgresql://localhost:5432/postgres?reWriteBatchedInserts=true`),
`SIM_DB_USER` and `SIM_DB_PASSWORD` **on the gateway only**. From the same directory used for the trading processes:

```bash
java -cp 'reporting-gateway/target/*:reporting-gateway/target/dependency/*' \
  com.helesto.journal.ProjectionGateway broker=data/broker/business-queue exchange=data/exchange/business-queue
```

One writer per stream batches history, current orders and the file-offset checkpoint in one SQL transaction.
The gateway resumes from that checkpoint after a restart. Trading can continue while SQL is offline.
After stopping the trading processes and letting the gateway catch up, use the same classpath and arguments with
`com.helesto.journal.ProjectionAudit` to compare the entire journal, order fields and reporting history.

The default journal uses OS-cached appends. Set `-Dsim.forceBusinessCommit=true` to force each business record before applying it.
QuickFIX/J uses a separate file store. Keep both stores together for restore. Confirm ambiguous sends before resuming a fenced session.
The admission check samples writer-usable disk space off the order thread. Low space (below 256 MiB) or a reading aged one second pauses new commands.
Fresh space reopens admission. The remaining headroom is available to complete accepted work. An actual uncertain write fences the session for recovery.

## Run with Docker Compose

Build with JDK 17, use the Angular 9 compatible Node toolchain for the dashboards and set `POSTGRES_PASSWORD` in the local shell.
Run `./run-with-local-build.sh`, or `./run-after-local-build.sh` when the JVM packages and dashboards are already built.
The two trading stores and reporting database have separate named volumes. Open the broker dashboard on port 80
and the exchange dashboard on port 90. Use a fresh Compose project name when evaluating against an empty database.

# Images

## Session Control

![image](./documentation/images/broker-session-control.png)

## Session Properties

![image](./documentation/images/broker-session-properties.png)

## Session Messages

![image](./documentation/images/broker-session-messages.png)

## Orders list

### Broker

![image](./documentation/images/broker-orders-list.png)

### Exchange

![image](./documentation/images/exchange-orders-list.png)

## Logs - FIX Events

![image](./documentation/images/broker-logs-events.png)

## Logs - Messages incoming

![image](./documentation/images/broker-logs-incoming-messages.png)

## Broker - Swagger

![image](./documentation/images/broker-swagger.png)

## Exchange - Swagger

![image](./documentation/images/exchange-swagger.png)



# Help Queries


```sql
select * from broker.sessions;
select * from broker.messages;
select * from broker.event_log  order by id desc;
select * from broker.messages_log_incoming order by id desc;
select * from broker.messages_log_outgoing order by id desc;

select * from exchange.sessions;
select * from exchange.messages;
select * from exchange.event_log  order by id desc;
select * from exchange.messages_log_incoming order by id desc;
select * from exchange.messages_log_outgoing order by id desc;
```

