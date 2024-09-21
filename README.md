# Smart City Real-Time Data Streaming

This project showcases a **Smart City Real-Time Data Streaming** pipeline built using a variety of technologies, including Kafka, Apache Spark, AWS S3, Glue, Redshift, and BI tools like Tableau and Power BI. The system simulates various data streams related to a city's infrastructure, processes the data in real-time, and visualizes it using analytics dashboards.

## System Architecture

![![Architecture diagram](https://github.com/user-attachments/assets/ab9d8879-7dfc-43f6-96a3-13c9b294e697)


## Project Overview

This project simulates and processes data streams from multiple sources such as:

- **Vehicle Information**
- **GPS Information**
- **Camera Information**
- **Weather Information**
- **Emergency Alerts**

The data is then streamed in real-time using **Kafka**, cleansed and transformed using **Apache Spark**, stored in AWS S3, and made queryable using AWS Glue and Athena. Finally, the data is visualized using BI tools like **Tableau** and **Microsoft Power BI**.

## Project Workflow

1. **Data Simulation:**
   - A Python simulator generates synthetic data related to vehicles, GPS, cameras, weather, and emergencies.
   - This data is sent to Kafka topics.

2. **Data Streaming with Kafka:**
   - Kafka acts as the messaging broker, streaming the data from various sources.
   - **Zookeeper** coordinates the Kafka brokers.

3. **Data Processing with Apache Spark:**
   - Apache Spark processes and cleanses the real-time data.
   - Spark Master and Workers handle distributed data processing.

4. **Data Storage in AWS S3:**
   - The processed data is stored in AWS S3 buckets:
     - **Raw Data Storage**
     - **Transformed Data Storage**

5. **Data Catalog with AWS Glue:**
   - AWS Glue creates a data catalog for the processed data in S3.
   - Glue Crawlers are used to populate the data catalog.

6. **Querying with Amazon Athena:**
   - The data catalog can be queried using **Amazon Athena** for quick insights.

7. **Data Warehouse with Amazon Redshift:**
   - The processed data is loaded into **Amazon Redshift** for advanced querying and analytics.

8. **Visualization using BI Tools:**
   - The Redshift data is connected to **Tableau** or **Microsoft Power BI** for visualization and reporting.

## Tech Stack

- **Python:** Data Simulator
- **Kafka:** Real-time data streaming
- **Apache Spark:** Data processing
- **AWS S3:** Data storage
- **AWS Glue:** Data cataloging
- **Amazon Athena:** SQL querying
- **Amazon Redshift:** Data warehouse
- **Tableau / Power BI:** Data visualization

## Setup and Installation

To replicate this project on your local machine or cloud environment, follow the steps below:

1. **Clone the repository:**
   ```bash
   git clone https://github.com/your-username/smart-city-realtime-data-streaming.git
   cd smart-city-realtime-data-streaming
