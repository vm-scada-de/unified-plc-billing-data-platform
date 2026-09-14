# Unified Data Platform: Commercial Accounting & Distributed Engineering Systems Monitoring

This project is a conceptual Industrial IoT (IIoT) and industrial automation (OT/IT) data pipeline. It is designed for monitoring distributed engineering systems and resource accounting. The repository demonstrates the end-to-end integration of operational technology (OT) field levels with enterprise data engineering and analytics tools.

## System Architecture

"""text
 ┌──────────────────┐   ┌──────────────────┐   ┌──────────────────┐   ┌──────────────────┐
 │   Vendor DB A    │   │   Vendor DB B    │   │  Siemens S7 PLC  │   │  Other Modbus    │
 │   (PostgreSQL)   │   │     (MS SQL)     │   │     (Tags)       │   │   Devices        │
 └────────┬─────────┘   └────────┬─────────┘   └────────┬────┬────┘   └────────┬────┬────┘
          │                      │                      │    │                 │    │
          │ (Meter Points)       │ (Meter Points)       │    │ (Meter Points)  │    │ (Meter Points)
          ▼                      ▼                      ▼    └────────┐        │    │
┌────────────────────────────────────────────────────────────┐        │        │    │
│                      Apache Airflow 3                      │◄───────┴────────┘    │
│              (Hourly ETL / Meter Points Batch)             │◄─────────────────────┘
└──────────────────────────────▲─────────────────────────────┘
                               │
                               │ ▲ (Read ETL Config)
                               │ ▼ (Write Processed Meter Points)
                               │
┌──────────────────────────────┴─────────────────────────────┐         ┌────────────────────────────────────┐
│                    PostgreSQL Database                     │────────►│            Web Portal UI           │
│    (Central Accounting, Master Data & Central Config)      │◄────────│  (Unified Monitoring & Analytics)  │
└──────────────────────────────┬─────────────────────────────┘ (Reports│                                    │
                               │                               & Config│    * Accounting & Meter Reports    │
                               │ (Read Tag & Config)           Changes)│    * Analytical Trends             │
                               ▼                                       │    * Live Stream Dashboard         │
                               ┌────────────────────────┐              └─────────────────────▲─────────▲────┘
                               │ Real-Time Poll Service │                                    │         │
                               │ (Configurable Archiver)│                                    │         │
                               └───────┬────────┬───────┘                                    │         │
                                       │        │                                            │         │
                        (Telemetry by  │        │ (Live Stream)                              │         │
                         Tag Settings) │        └────────────────────────────────────────────┘         │
                                       ▼                                                               │
                               ┌──────────────┐                                                        │
                               │  ClickHouse  │────────────────────────────────────────────────────────┘
                               │ (Time-Series)│ (Trends)                                               
                               └──────────────┘                                                        
"""

## Data Processing & Ingestion Layers

The platform architecture is divided into two independent data loops (batch ingestion and real-time streaming). The behavior of both loops is fully driven by a centralized configuration model.

### 1. Central Storage & Configuration Module (PostgreSQL)
*   **The System Core:** The PostgreSQL database serves not only as a consolidated datastore for accounting data and Master Data Management (MDM) but also acts as the **Central Configuration (Central Config)** engine for the entire platform.
*   **Component Management:** All ETL schedules, Modbus device polling parameters, register addresses, and individual storage archiving intervals for each specific tag are stored within PostgreSQL. Any configuration updates submitted by an operator via the Web UI are instantly picked up and applied by the ingestion services.

### 2. Hourly Batch Ingestion (Managed by Apache Airflow 3)
*   **Scheduled ETL Process:** Every hour, the Apache Airflow 3 orchestrator reads the layout parameters from the central configuration database and triggers automated pipeline DAGs to extract resource consumption metrics from Meter Points.
*   **Data Sources:** Batch data ingestion of aggregated historical counters and pulse metrics is executed simultaneously from four source types:
    *   Vendor Accounting System A (PostgreSQL database)
    *   Vendor Accounting System B (MS SQL Server database)
    *   Direct registers of meter points inside Siemens S7 PLCs
    *   Adjacent peripheral devices via Modbus TCP
*   **Consolidation & Storage:** Extracted data is validated, cleaned, standardized, and written back into the central **PostgreSQL** database to generate balancing and accounting reports.

### 3. Real-Time Telemetry Streaming (Custom Python Service)
*   **Dynamic Tag Polling:** A dedicated, lightweight background Python worker continuously polls technological parameters and raw values **directly from the tags of Siemens S7 PLCs** and other Modbus devices.
*   **Configurable Archiver:** The service utilizes a dynamic time-series model. The frequency and conditions for committing data records to the storage layer are configured individually for each tag based on the live settings pulled from the PostgreSQL central config.
*   **Dual-Streaming Routing:**
    *   *Storage Layer:* High-frequency process telemetry is committed to a column-oriented **ClickHouse** database, heavily optimized for time-series analytical queries.
    *   *Presentation Layer:* The raw "live" tag values (Live Stream) bypass database overhead and are instantly streamed to the web interface via low-latency protocols for real-time SCADA-like UI animations.

### 4. Presentation Layer (Web Portal UI)
*   **Unified Web Portal:** An operator dashboard that seamlessly unifies engineering metrics and commercial balance charts (from PostgreSQL), analytical time-series trends (from ClickHouse), and live animation of the technological stream into a single browser window. The portal also features an administrative panel to modify system configurations inside PostgreSQL.



