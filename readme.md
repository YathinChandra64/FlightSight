✈️ FlightSight: Uncovering Revenue & Route Trends in Aviation

📖 Overview
FlightSight addresses the challenge of fragmented, high-volume aviation datasets by delivering an end-to-end data pipeline that ingests, transforms, and visualizes flight and revenue data. Our solution empowers stakeholders to identify revenue opportunities, optimize route performance, and make data-driven decisions with confidence.

🛑 Problem Statement
Airlines and travel operators contend with disparate, large-scale datasets, which hinder timely analysis and strategic planning. FlightSight streamlines this process by consolidating, processing, and analyzing multi-source aviation data to surface actionable trends in revenue and route performance.

🛠️ Resources & Specifications
Data Sources:

Amadeus API: Flight schedules, booking volumes

OpenWeather API: Route-wise weather metrics

Platform & Tools:

Cloud & Orchestration: Azure Data Lake Gen2, Azure Data Factory

Compute & Transform: Azure Databricks (PySpark)

Storage & Query: Snowflake Data Warehouse

Visualization: Power BI Desktop

🏗️ Solution Architecture

Core Components
Azure Data Lake Gen2: Landing zone for raw CSV and JSON files

Azure Data Factory: Orchestrates ingestion, metadata extraction, and workflow execution

Azure Databricks: Transforms and enriches data (cleaning, type casting, feature engineering)

Snowflake: Hosts curated, analytics-ready tables (silver/gold layers)

Power BI: Interactive dashboards for revenue and route analysis

🔄 Implementation Workflow
Ingest: Raw data uploaded to ADLS Gen2 via S3-compatible interface

Orchestrate: ADF pipeline triggers on arrival, invokes Get Metadata and ForEach activities

Stage: Landing data stored in bronze layer; automated schema detection handles dynamic files

Transform: Databricks notebooks apply data quality checks, enrich with weather factors, and persist to Snowflake silver/gold

Load & Model: ADF executes SQL scripts/stored procedures to create derived views and fact tables

Monitor & Alert: Web activity sends notifications on pipeline failures or SLA breaches

Visualize: Power BI datasets leverage Snowflake views for real-time slicing, filtering, and drill-down

📊 Key Results & Insights
Route Profitability: Identified top-performing routes by revenue-per-flight and seat-occupancy metrics

Operational Efficiency: Revealed weekday vs. weekend scheduling opportunities, improving asset utilization

Weather Risk Analysis: Correlated adverse weather patterns with delay probabilities to inform contingency planning

🚀 Future Enhancements
Streaming Ingestion: Shift from micro-batch to real-time data feeds using Azure Event Hubs

Expanded Data Sources: Incorporate booking platforms, customer feedback, and ancillary revenue streams

Advanced Analytics: Deploy ML models for demand forecasting and dynamic pricing optimization

📚 References
Azure Data Factory Copy Activity Overview

Azure Data Factory Script & Web Activities Guide

Snowflake Stored Procedures & Best Practices

📬 Contact
B. Yathin Chandra
Email: yathinchandra985@gmail.com