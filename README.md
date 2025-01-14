Project Overview
This repository contains components designed to manage data ingestion, transformation, and visualization in a robust data pipeline. Below is a breakdown of its structure and functionality:

streaming/ Directory:
This framework manages real-time data ingestion into a destination database ( MySQL). It follows a modular structure with packages, each containing multiple modules, and each module implementing specific functions to ensure seamless data integration.

etl/ Directory:
Similar in structure to the streaming/ directory, this framework focuses on batch data processing. It reads data from MySQL, applies transformations, and then loads the processed data into a data warehouse (Hive or Snowflake). This dual implementation demonstrates two distinct solutions for data warehousing.

docker-hive-master/ Directory:
Contains configurations for deploying Hive-based data warehouse services. The configuration is adapted from the widely recognized European Big Data GitHub repository and modified to suit specific resources and requirements.

dashboard/ Directory:
This directory houses visualization tools:

Power BI Dashboards provide detailed insights and analytics.
Streamlit Applications are under development to enable real-time data visualization and insights.
Plans are underway to build additional applications using advanced tools and technologies such as Django and JavaScript, aimed at enhancing real-time analytics capabilities.

Configuration Files (hive.py and sql.py):
These files define table creation and data definitions for both the database (MySQL) and the data warehouse (Hive or Snowflake). However, tasks like constraints and data verification are managed directly via the CLI of the respective environments.