import requests
import os
from dotenv import load_dotenv
import pandas as pd
from sqlalchemy import create_engine, text
from openai import OpenAI
import json

load_dotenv()


class MiroBoardManager:
    def __init__(self):
        self.access_token = os.getenv("MIRO_ACCESS_TOKEN")
        self.base_url = "https://api.miro.com/v2"
        self.headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json",
        }
        self.engine = create_engine("postgresql://airflow:airflow@postgres/airflow")
        self.llm_client = OpenAI(
            api_key=os.getenv("OPENAI_API_KEY"), base_url="https://api.deepseek.com"
        )

    def create_board(self, name):
        url = f"{self.base_url}/boards"
        payload = {"name": name, "sharingPolicy": {"access": "private"}}
        response = requests.post(url, headers=self.headers, json=payload)
        response.raise_for_status()
        return response.json()["id"]

    def create_sticky_note(self, board_id, text, x, y):
        url = f"{self.base_url}/boards/{board_id}/sticky_notes"
        payload = {"data": {"content": text}, "position": {"x": x, "y": y}}
        response = requests.post(url, headers=self.headers, json=payload)
        response.raise_for_status()
        return response.json()["id"]

    def get_affinity_groups(self, items):
        prompt = f"""
        As a UX designer, analyze these items and group them into logical affinity groups.
        Create meaningful group names that capture the essence of each cluster.
        
        Items to analyze: {items}
        
        Format response as JSON with this structure:
        {{
            "groups": [
                {{
                    "name": "group name",
                    "items": ["item1", "item2"]
                }}
            ]
        }}
        """

        response = self.llm_client.chat.completions.create(
            model="deepseek-chat",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.7,
        )

        content = response.choices[0].message.content
        if content.startswith("```json"):
            content = content[7:-3]
        return json.loads(content.strip())

    def create_affinity_board(self, board_name):
        # Create a new board
        board_id = self.create_board(board_name)

        # Get unprocessed records from database
        with self.engine.connect() as conn:
            # Add miro_scanned column if it doesn't exist
            conn.execute(
                text(
                    """
                ALTER TABLE analyzed_comments 
                ADD COLUMN IF NOT EXISTS miro_scanned VARCHAR(1) DEFAULT 'N'
            """
                )
            )
            conn.commit()

            # Get unprocessed records
            df = pd.read_sql(
                "SELECT * FROM analyzed_comments WHERE miro_scanned = 'N'", conn
            )

        # Collect all items for each category
        pain_points = []
        gain_points = []
        jobs = []

        for _, row in df.iterrows():
            if row["pain_points"]:
                pain_points.extend([p.strip() for p in row["pain_points"].split(",")])
            if row["gain_points"]:
                gain_points.extend([g.strip() for g in row["gain_points"].split(",")])
            if row["jobs_to_be_done"]:
                jobs.extend([j.strip() for j in row["jobs_to_be_done"].split(",")])

        # Get affinity groups for each category
        pain_groups = self.get_affinity_groups(pain_points)
        gain_groups = self.get_affinity_groups(gain_points)
        jobs_groups = self.get_affinity_groups(jobs)

        # Create columns for affinity mapping
        columns = ["Pain Points", "Gain Points", "Jobs to be Done"]
        column_positions = [-1000, 0, 1000]  # x-coordinates for columns
        groups_data = [pain_groups, gain_groups, jobs_groups]

        for col, x, groups in zip(columns, column_positions, groups_data):
            # Create column header
            self.create_sticky_note(board_id=board_id, text=col, x=x, y=-500)

            # Add grouped items
            y_offset = -300
            for group in groups["groups"]:
                # Create group header
                self.create_sticky_note(
                    board_id=board_id, text=f"[GROUP] {group['name']}", x=x, y=y_offset
                )
                y_offset += 100

                # Add items in group
                for item in group["items"]:
                    self.create_sticky_note(
                        board_id=board_id, text=item, x=x, y=y_offset
                    )
                    y_offset += 100

                y_offset += 100  # Extra space between groups

        # Mark records as processed
        with self.engine.connect() as conn:
            conn.execute(
                text(
                    "UPDATE analyzed_comments SET miro_scanned = 'Y' WHERE miro_scanned = 'N'"
                )
            )
            conn.commit()

        return board_id


if __name__ == "__main__":
    manager = MiroBoardManager()
    board_id = manager.create_affinity_board("Reddit Analysis")
    print(f"Created Miro board with ID: {board_id}")
