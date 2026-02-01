Supply Chain Big Data Analytics Pipeline

This project implements a complete Big Data pipeline for processing and analyzing supply chain data using:

Kafka – Real-time data ingestion

HDFS – Distributed raw + processed data storage

Apache Spark – Data cleaning, transformation, and analytics

Parquet – Optimized analytical storage

Streamlit Dashboard – Visualizing the final KPIs 

End-to-End Pipeline Architecture

                ┌─────────────────────────┐
                │      Data collection    │
                │                         │  
                └────────────┬────────────┘
                             │
                             ▼
             ┌─────────────────────────────┐
             │   Kafka Data Ingestion      │
             │                             │
             └────────────┬────────────────┘
                             │
                             ▼
             ┌────────────────────────────┐
             │  Raw Storage in HDFS       │
             │  
             └────────────┬───────────────┘
                             │
                             ▼
     ┌──────────────────────────────────────────┐
     │ Spark Data Cleaning & Feature Engineering │
     │ • Trim whitespaces                        │
     │ • Fix data types                          │
     │ • Convert dates                            │
     │ • Calculate delivery delay                  │
     │ Output → HDFS Parquet                      │
     │                                           │
     └────────────┬────────────────────────────┘
                             │
                             ▼
     ┌──────────────────────────────────────────┐
     │ Spark SQL Analytics                       │
     │ • Avg delay by product                    │
     │ • Supplier performance                    │
     │ • Cost and carrier performance            │
     │ Output → Parquet                          │
     │                                           │
     └────────────┬────────────────────────────┘
                             │
                             ▼
     ┌──────────────────────────────────────────┐
     │ Streamlit Dashboard                       │
     │ • Reads analytics output (Parquet/CSV)    │
     │ • KPI dashboards + charts                 │
     └──────────────────────────────────────────┘


 Spark: Data Cleaning

supply_chain_cleaning.py performs:

✔ Load raw HDFS data
✔ Clean & standardize fields
✔ Convert date strings → date type
✔ Create delivery_delay_days
✔ Save processed data back to HDFS (Parquet)


Spark: KPI Analytics

supply_chain_analytics.py generates:

 Average delay per product

 Shipping carrier KPIs

 Supplier performance

 Transportation mode costs




Streamlit Dashboard 

It visualizes:

Product-wise delays

Supplier KPIs

Shipment cost trends

Transportation mode effectiveness



🛠️ Technologies Used

Layer	Technology

Ingestion	Apache Kafka
Storage	HDFS
Processing	PySpark
Analytics	Spark SQL
Visualization	Streamlit
Data Format	CSV + Parquet
VM	Ubuntu Linux (VirtualBox)
