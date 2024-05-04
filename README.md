#Multinational Retail Data Centralisation

Overview
In today's globalized retail landscape, managing sales data efficiently is critical for informed decision-making. The Multinational Retail Data Centralisation project aims to address the challenge of disparate data sources by centralizing sales data into a single, accessible location. This repository contains tools and scripts to extract, clean, and centralize sales data from various sources into a centralized database, enabling streamlined analysis and reporting.

Key Components
1. Database Connector
The data_utils.py file provides the DatabaseConnector class, facilitating connections to both the source and local databases. This class enables seamless extraction and transfer of data from source to destination.

2. Data Extraction
The data_extraction.py file houses the DatabaseExtractor class, responsible for extracting data from diverse sources such as databases, PDF files, and JSON files. This component ensures comprehensive data retrieval for subsequent processing.

3. Data Cleaning
The data_cleaning.py file features the DatabaseCleaning class, which handles data cleaning tasks based on specific requirements. This component ensures data consistency and accuracy before integration into the centralized database.

4. Project Execution
The mrdc_project.ipynb file serves as the main project execution script, orchestrating the data extraction, cleaning, and transfer processes. This Jupyter Notebook integrates the aforementioned components to create a streamlined workflow for centralizing sales data.

5. Creating Database Schema
This SQL file (create_schema.sql) contains statements to adjust data types in the tables and set up foreign key constraints between the orders_table and related dimension tables. 
The initial section of the SQL file focuses on adjusting data types in the orders_table and related dimension tables to ensure compatibility and consistency across the database schema. This involves altering column types and lengths based on data requirements and standards. 
Following the data type adjustments, primary keys are added to the dimension tables (dim_users, dim_store_details, dim_dates_times, dim_card_details) to uniquely identify each record. Primary keys are essential for data indexing and maintaining data integrity within the database.
Finally, foreign key constraints are established in the orders_table to reference the primary keys in the dimension tables. These constraints enforce referential integrity, ensuring that data in the orders_table accurately corresponds to records in the associated dimension tables.

6. Querying the Data
The query_data.sql file contains a collection of SQL queries designed to analyze sales data for the company. These queries cover various aspects of sales, including total sales by country, store type, and product, as well as metrics such as average time between sales. The queries are structured to provide insightful information to the sales team, operations team, and management for strategic decision-making.

Getting Started
Clone the repository:
bash
git clone https://github.com/JenHolmes608/multinational-retail-data-centralisation246.git
Navigate to the project directory:
bash
cd multinational-retail-data-centralisation246
Ensure Python is installed.
Install dependencies:
bash
pip install -r requirements.txt
Set up the necessary credentials and configurations in a YAML file.
Execute the project script:
bash
python mrdc_project.py
Configuration
Define credentials for data extraction and upload destinations in a YAML file.
Ensure connectivity to AWS and define the appropriate S3 bucket.
Provide the endpoint URL or JSON URL for data extraction.
For PDF data extraction, include the relevant link to the PDF file.
Contributing
Contributions to this project are welcome! Please follow the standard guidelines for contributing.