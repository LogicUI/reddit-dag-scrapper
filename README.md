# Reddit Analysis Pipeline

This project implements a daily pipeline for scraping Reddit posts, analyzing them using Anthropic's Claude, creating Miro boards for affinity mapping, and storing the results in PostgreSQL.

## Features

- Daily scraping of Reddit posts with specific tags (discussion, question, recommendation)
- LLM analysis of posts to identify pain points, gain points, and jobs to be done
- Automatic creation of Miro boards with affinity mapping
- PostgreSQL storage for historical analysis

## Prerequisites

- Docker and Docker Compose
- Reddit API credentials
- Anthropic API key
- Miro API access token

## Setup

1. Clone this repository
2. Copy `.env.example` to `.env` and fill in your API credentials:
   ```bash
   cp .env.example .env
   ```
3. Build and start the containers:
   ```bash
   docker-compose up -d
   ```
4. Access the services:
   - Airflow UI: http://localhost:8080
   - pgAdmin: http://localhost:8081
     - Login with admin@admin.com / admin
     - Add a new server with host: postgres, port: 5432, database: reddit_analysis

## Project Structure

- `dags/`: Contains the Airflow DAG and supporting Python modules
  - `reddit_scraper.py`: Reddit scraping functionality
  - `llm_analyzer.py`: LLM analysis using Anthropic's Claude
  - `miro_integration.py`: Miro board creation and management
  - `reddit_analysis_dag.py`: Main Airflow DAG orchestrating the workflow

## Usage

1. The pipeline runs automatically daily at midnight
2. You can also trigger it manually through the Airflow UI
3. View the results in:
   - Miro boards (automatically created)
   - PostgreSQL database (accessible via pgAdmin)

## Monitoring

- Check the Airflow UI for task status and logs
- Monitor the PostgreSQL database for stored data
- View created Miro boards in your Miro account 