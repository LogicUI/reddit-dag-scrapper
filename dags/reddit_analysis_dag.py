from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from reddit_scraper import RedditScraper
from llm_analyzer import LLMAnalyzer
from miro_integration import MiroBoardManager
import pandas as pd
from sqlalchemy import create_engine

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def scrape_reddit():
    """Task 1: Scrape Reddit comments with error handling"""
    try:
        scraper = RedditScraper()
        df = scraper.scrape_subreddit_comments(
            "journaling", limit=50
        )  # Reduced limit for testing
        if df.empty:
            print("No comments found matching criteria")
        return df
    except Exception as e:
        print(f"Error scraping Reddit comments: {str(e)}")
        raise


def analyze_posts(**context):
    ti = context["ti"]
    df = ti.xcom_pull(task_ids="scrape_reddit")

    # Connect to the database
    engine = create_engine("postgresql://airflow:airflow@postgres/airflow")

    # Get existing post IDs from the database
    existing_posts_query = "SELECT post_id FROM reddit_posts"
    existing_posts = pd.read_sql(existing_posts_query, engine)

    # Filter out posts that already exist in the database
    if not existing_posts.empty:
        df = df[~df["id"].isin(existing_posts["post_id"])]

    # Only proceed with analysis if there are new posts
    if df.empty:
        print("No new posts to analyze")
        return pd.DataFrame()  # Return empty DataFrame if no new posts

    # Analyze only the new posts
    analyzer = LLMAnalyzer()
    analyzed_df = analyzer.analyze_dataframe(df)

    return analyzed_df


def create_miro_board(**context):
    ti = context["ti"]
    df = ti.xcom_pull(task_ids="analyze_posts")
    manager = MiroBoardManager()
    board_id = manager.create_affinity_board(
        f"Reddit Analysis - {datetime.now().strftime('%Y-%m-%d')}", df
    )
    return board_id


def store_in_postgres(**context):
    ti = context["ti"]
    df = ti.xcom_pull(task_ids="analyze_posts")

    engine = create_engine("postgresql://airflow:airflow@postgres/reddit_analysis")
    df.to_sql("reddit_posts", engine, if_exists="append", index=False)


with DAG(
    "reddit_analysis",
    default_args=default_args,
    description="Daily Reddit analysis pipeline",
    schedule_interval="0 0 * * *",  # Run daily at midnight
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:

    scrape_task = PythonOperator(
        task_id="scrape_reddit",
        python_callable=scrape_reddit,
    )

    analyze_task = PythonOperator(
        task_id="analyze_posts",
        python_callable=analyze_posts,
    )

    miro_task = PythonOperator(
        task_id="create_miro_board",
        python_callable=create_miro_board,
    )

    store_task = PythonOperator(
        task_id="store_in_postgres",
        python_callable=store_in_postgres,
    )

    scrape_task >> analyze_task >> [miro_task, store_task]
