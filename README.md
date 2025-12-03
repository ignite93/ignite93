# Thmanyah – Real-Time Engagement Pipeline  
(Concept Data Engineering Project)

## Overview

This project is a simple but complete real-time data pipeline built using Kafka, Spark Structured Streaming, PostgreSQL, Docker, and Python.  
It simulates user engagement events, processes them in real time, stores them in a database, and then runs analytics to understand user behavior.

The idea is to show how I think about building a data pipeline from end to end:  
from event generation, to real-time processing, to storage, to reporting.

---

## Why I Built This

This project is part of a technical evaluation, and I wanted it to be:

- Easy to run.  
- Close to real-world data engineering work.  
- Clear in how the pieces fit together.

While building it, I:

- Deepened my understanding of Kafka and Spark Streaming.  
- Connected the full flow: ingestion → processing → storage → analytics.  
- Improved my debugging skills across multiple services (Kafka, Spark, Postgres, Docker).  
- Turned theoretical knowledge into something practical and working.

I am also actively studying and practicing:

- Data Analytics  
- Data Engineering  
- Snowflake  
- Cloud data architecture  

Joining a Saudi company like Thmanyah would be a big honor for me, and I am confident in my ability to learn fast and contribute in a real way.

---

## Architecture

High-level data flow:

Event Producer → Kafka → Spark Structured Streaming → PostgreSQL → Analytics Reports

---

## Simple Explanation

- A Python script generates random events that look like real user engagement.  
- Kafka receives these events as a continuous stream.  
- Spark Structured Streaming reads from Kafka and aggregates the events in real time.  
- PostgreSQL stores the processed data and raw events for analysis.  
- The script `analytics_report.py` connects to the database and prints useful analytics.

---

## Components

**Kafka**  
Receives real-time events on a single topic named `engagement_stream`.

**Spark Structured Streaming**  
Reads the stream from Kafka, parses JSON into columns, and runs windowed aggregations over time.

**PostgreSQL**  
Stores data in structured tables for analysis.  
Main tables used in this project are:

- `content`  
- `engagement_events`

**Redis (optional)**  
Available as an optional cache layer for future experiments or extensions.

**Docker Compose**  
Runs the full environment with one command:

- Kafka  
- Zookeeper  
- PostgreSQL  
- Redis  

**Python**  
All project scripts are written in Python:

- `Event_processing/events_producer.py` – sends events to Kafka.  
- `Event_processing/events_stream_job.py` – Spark streaming job that reads from Kafka.  
- `Event_processing/analytics_report.py` – analytics report directly from PostgreSQL.

---

## Features

- Real-time streaming of engagement events into Kafka.  
- JSON parsing and schema handling inside Spark.  
- One-minute window aggregations by time, `content_id`, and `event_type`.  
- Clean storage of events and aggregates in PostgreSQL.  
- A simple analytics script that prints key metrics in a readable format.  
- A structure that is easy to extend with more metrics, dimensions, or sinks.

---

## Design decisions

I made a few simple but intentional design choices in this project:

- **Kafka instead of writing straight to PostgreSQL**  
  To simulate a real streaming setup where producers and consumers are decoupled.  
  This makes the system easier to scale and reason about than sending events directly to the database.

- **Spark Structured Streaming instead of a simple consumer script**  
  Spark gives built-in support for windowed aggregations, fault tolerance, and a clear streaming API.  
  It also makes it easy to extend the logic later with more transformations.

- **Using a 1-minute time window**  
  This is small enough to feel “real-time”, but large enough to show meaningful aggregation per content and event type.

- **PostgreSQL as the main storage layer**  
  It is easy to run in Docker, familiar for analytics, and works well with simple SQL reports.  
  It also lets me demonstrate how the streaming layer feeds into an analytical store.

- **Spark writing to a console sink (not directly to PostgreSQL)**  
  For this assignment, printing to the console keeps the setup simple and transparent.  
  You can see the streaming aggregations live without extra configuration or migrations.

- **Randomly generated test events**  
  The focus here is on the pipeline design and behavior, not on realistic production data.  
  Generating events lets me quickly test different patterns and volumes without external systems.

- **Docker Compose for all services**  
  Instead of asking you to install Kafka, Zookeeper, PostgreSQL, and Redis manually,  
  Docker Compose makes the whole environment reproducible with a single command.

Overall, the goal was not to over-engineer the system, but to build a clear, honest, and extendable real-time data pipeline that reflects how I think about data engineering in practice.

---

## Requirements

- Python 3.10 or higher  
- Docker Desktop  
- Docker Compose  
- JDK 17 (Temurin)  
- PySpark 3.4.1 with Kafka integration  
  (managed via `spark.jars.packages` inside `events_stream_job.py`)

---

## How to Run the Project

> Note: the paths below are based on my local machine. You can adjust them to match your setup.

### 1) Navigate to the project and activate the virtual environment

```powershell
cd "C:\Users\user\Desktop\Abdulaziz Alkhateeb - thmanyah project"
.\.venv\Scripts\Activate.ps1
```
### 2) Start all services (Kafka, Postgres, Redis, Zookeeper)

```powershell
docker compose up -d
```

This will start the following containers:

- `thmanyah_kafka`  
- `thmanyah_zookeeper`  
- `thmanyah_postgres`  
- `thmanyah_redis`  

### 3) Run the event producer

This script sends random engagement events to the Kafka topic `engagement_stream`:

```powershell
python Event_processing/events_producer.py
```

You should see lines like:

```text
Sent: {"event_id": "...", "content_id": "...", ...}
```

### 4) Run the Spark streaming job

This script reads from Kafka, parses the JSON, and performs 1-minute window aggregations by `content_id` and `event_type`:

```powershell
python Event_processing/events_stream_job.py
```

The results will appear in the console as batches, for example:

```text
-------------------------------------------
Batch: 2
-------------------------------------------
+------------------------------------------+------------------------------------+----------+-----+
|window                                    |content_id                          |event_type|count|
+------------------------------------------+------------------------------------+----------+-----+
|{2025-12-03 00:56:00, 2025-12-03 00:57:00}|8740f0a2-a248-4538-822f-76c1ddc7af38|pause     |1    |
...
```

### 5) Generate the analytics report

This script connects to PostgreSQL and prints several useful aggregates:

```powershell
python Event_processing/analytics_report.py
```

The report includes for example:

- Total events per `event_type`.  
- Top content by total engagement.  
- Completion rate (finish / play) per content.  
- Events per device (ios / android / chrome / web-safari).

---

## Example Insights

Using this pipeline, we can answer questions like:

- **Top content by engagement**  
  Which `content_id` has the highest number of events?

- **Completion rate per content**  
  How often a piece of content is finished compared to how often it is played.

- **Events per device**  
  How events are distributed between ios, android, chrome, and web-safari.

- **Play events over recent time windows**  
  For example: plays in the last 30 minutes.

- **Distribution of event types**  
  How often we see play vs pause vs click vs finish.

This is just a starting point. It is easy to add:

- Analysis by `user_id`.  
- Analysis by content type (podcast / newsletter / video).  
- Deeper insights like retention or session-level behavior.

---

## Assumptions

- All events are randomly generated test data. The goal is to demonstrate the pipeline, not real production traffic.  
- There is no authentication or advanced security layer; the focus is on data engineering, not a full product.  
- PostgreSQL is the main target store for analytics in this project.  
- Spark writes aggregated results to the console sink to keep evaluation simple, without external sinks.  
- Scripts are run manually (no scheduler like Airflow) to keep the project small and easy to understand.

---

## Possible Future Improvements

If there is more time or a larger environment, this project could be extended with:

- A data lake (for example, S3) to store raw and processed data.  
- An orchestrator like Airflow to schedule ingestion and processing jobs.  
- Better monitoring and logging (Prometheus / Grafana).  
- A more advanced schema design in PostgreSQL (partitioning, indexing).  
- Integration with BI tools such as Looker, Metabase, or Power BI.  
- A small API layer on top of the analytics to power dashboards.

---

## Project Structure

```text
.
├── Event_processing/
│   ├── events_producer.py
│   ├── events_stream_job.py
│   └── analytics_report.py
├── docker-compose.yml
├── Dockerfile
├── requirements.txt
└── README.md
```

---

## Final Note

I built this project to be:

- Clear in its idea.  
- Simple to run.  
- Close to how I like to work day to day.

I enjoy building things that are real and working, even if they are small, and I focus on:

- Making them correct.  
- Making them easy to extend later.

I hope this project gives a good picture of how I think about data engineering, how I learn, and how I solve problems in practice.

Thank you for your time.  
**Abdulaziz Alkhateeb**
