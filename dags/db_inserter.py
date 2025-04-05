import pandas as pd
import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values


class DBInserter:
    def __init__(
        self, dbname="airflow", user="airflow", password="airflow", host="localhost"
    ):
        self.conn_params = {
            "dbname": dbname,
            "user": user,
            "password": password,
            "host": host,
        }

    def _get_connection(self):
        return psycopg2.connect(**self.conn_params)

    def update_column_from_csv(self, csv_path, column_name):
        """
        Updates a single column from CSV file to the staging table

        Args:
            csv_path (str): Path to the CSV file
            column_name (str): Name of the column to update
        """
        # Read CSV file
        df = pd.read_csv(csv_path)

        conn = self._get_connection()
        cur = conn.cursor()

        try:
            # Update staging table
            for idx, row in df.iterrows():
                update_query = sql.SQL(
                    """
                    UPDATE analyzed_comments_staging 
                    SET {} = %s
                    WHERE comment_hash = %s
                """
                ).format(sql.Identifier(column_name))

                cur.execute(update_query, (row[column_name], row["comment_hash"]))

            conn.commit()
            print(f"Successfully updated {column_name} column in staging table")

        except Exception as e:
            conn.rollback()
            print(f"Error updating column {column_name}: {str(e)}")
            raise
        finally:
            cur.close()
            conn.close()

    def merge_staging_to_main(self):
        """Merges records from analyzed_comments_staging into analyzed_comments"""
        conn = self._get_connection()
        cur = conn.cursor()

        try:
            # Get staging table columns
            cur.execute(
                """
                SELECT column_name, data_type 
                FROM information_schema.columns
                WHERE table_name = 'analyzed_comments_staging'
                ORDER BY ordinal_position
            """
            )
            staging_columns = cur.fetchall()

            # Get main table columns
            cur.execute(
                """
                SELECT column_name, data_type
                FROM information_schema.columns 
                WHERE table_name = 'analyzed_comments'
                ORDER BY ordinal_position
            """
            )
            main_columns = cur.fetchall() or []

            # Create main table if it doesn't exist
            if not main_columns:
                create_cols = [f"{col} {dtype}" for col, dtype in staging_columns]
                create_sql = f"""
                    CREATE TABLE analyzed_comments (
                        {', '.join(create_cols)},
                        CONSTRAINT analyzed_comments_pkey PRIMARY KEY (comment_hash)
                    )
                """
                cur.execute(create_sql)
                main_columns = staging_columns

            # Add any missing columns to main table
            staging_col_names = [col for col, _ in staging_columns]
            main_col_names = [col for col, _ in main_columns]

            for col, dtype in staging_columns:
                if col not in main_col_names:
                    cur.execute(
                        f"""
                        ALTER TABLE analyzed_comments 
                        ADD COLUMN {col} {dtype}
                    """
                    )

            # Build dynamic merge query using available columns
            update_cols = [
                f"{col} = EXCLUDED.{col}"
                for col in staging_col_names
                if col != "comment_hash"
            ]

            merge_sql = f"""
                INSERT INTO analyzed_comments ({', '.join(staging_col_names)})
                SELECT {', '.join(staging_col_names)} 
                FROM analyzed_comments_staging
                ON CONFLICT (comment_hash) 
                DO UPDATE SET {', '.join(update_cols)}
            """
            cur.execute(merge_sql)

            # Truncate staging table
            cur.execute("TRUNCATE TABLE analyzed_comments_staging")

            conn.commit()
            print("Successfully merged staging data into main table")

        except Exception as e:
            conn.rollback()
            print(f"Error merging tables: {str(e)}")
            raise
        finally:
            cur.close()
            conn.close()

    def insert_analyzed_comments_staging(self, csv_path):
        # Read the CSV file
        df = pd.read_csv(csv_path)

        # Create database connection
        conn = self._get_connection()
        cur = conn.cursor()

        # Create staging table if it doesn't exist
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS analyzed_comments_staging (
            id VARCHAR,
            content TEXT,
            author VARCHAR, 
            created_utc TIMESTAMP,
            score INTEGER,
            permalink TEXT,
            subreddit VARCHAR,
            parent_id VARCHAR,
            is_submitter BOOLEAN,
            post_title TEXT,
            post_hash VARCHAR,
            comment_hash VARCHAR PRIMARY KEY,
            scanned VARCHAR(1),
            pain_points TEXT,
            gain_points TEXT,
            jobs_to_be_done TEXT,
            themes TEXT,
            relevance_score FLOAT,
            ideal_features TEXT
        )
        """
        cur.execute(create_table_sql)

        # Insert data in chunks
        chunk_size = 500
        for i in range(0, len(df), chunk_size):
            chunk = df.iloc[i : i + chunk_size]

            columns = list(df.columns)
            values = [tuple(x) for x in chunk.values]

            insert_query = sql.SQL(
                """
                INSERT INTO analyzed_comments_staging ({})
                VALUES %s
                ON CONFLICT (comment_hash) DO NOTHING
            """
            ).format(sql.SQL(",").join(map(sql.Identifier, columns)))

            # Execute the insert
            execute_values(cur, insert_query, values)

        # Commit and close
        conn.commit()
        cur.close()
        conn.close()


if __name__ == "__main__":
    db_inserter = DBInserter()
    db_inserter.insert_analyzed_comments_staging("analyzed_journaling_comments.csv")
