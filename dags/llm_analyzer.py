import os
from dotenv import load_dotenv
import pandas as pd
import json
import time
from openai import OpenAI

load_dotenv()


class LLMAnalyzer:
    def __init__(self):
        self.client = OpenAI(
            api_key=os.getenv("OPENAI_API_KEY"), base_url="https://api.deepseek.com"
        )

    def analyze_post(self, content):
        prompt = f"""
        Analyze the following Reddit comment and identify:
        1. Pain points mentioned
        2. Gain points (benefits or positive aspects) 
        3. Jobs to be done (what the user is trying to accomplish)
        4. Key themes/tags for affinity mapping
        5. Relevance score (0.1-1.0) for building an AI journaling app, where:
           - 1.0: Highly relevant insights about journaling habits, needs and pain points
           - 0.5: Moderately useful general journaling discussion
           - 0.1: Not relevant for AI journaling app development
        
        Content: {content}
        
        Format your response as JSON with these keys:
        - pain_points: list of pain points
        - gain_points: list of gain points 
        - jobs_to_be_done: list of jobs
        - themes: list of key themes/tags
        - relevance_score: float between 0.1 and 1.0
        """

        max_retries = 3
        retry_delay = 2  # seconds

        for attempt in range(max_retries):
            try:
                response = self.client.chat.completions.create(
                    model="deepseek-chat",
                    messages=[{"role": "user", "content": prompt}],
                    temperature=0.7,
                )

                # Get the response content and clean it up
                message_content = response.choices[0].message.content
                print(f"Message content: {message_content}")

                # Remove markdown code blocks if present
                cleaned_content = message_content.strip()
                if cleaned_content.startswith("```json"):
                    cleaned_content = cleaned_content[7:]  # Remove ```json
                if cleaned_content.endswith("```"):
                    cleaned_content = cleaned_content[:-3]  # Remove ```
                cleaned_content = cleaned_content.strip()

                analysis = json.loads(cleaned_content)
                print(
                    f"Successfully analyzed post. Analysis: {json.dumps(analysis, indent=2)}"
                )
                return analysis
            except Exception as e:
                if attempt < max_retries - 1:
                    print(f"Attempt {attempt + 1} failed: {str(e)}")
                    print(f"Retrying in {retry_delay} seconds...")
                    time.sleep(retry_delay)
                    retry_delay *= 2  # Exponential backoff
                else:
                    print(f"Failed after {max_retries} attempts: {str(e)}")
                    return None

    def create_analyzed_column(self, csv_path, new_column, analysis_prompt):
        """
        Analyze a CSV file and add a new column with LLM analysis results.

        Args:
            csv_path (str): Path to the CSV file
            new_column (str): Name of the column to add
            analysis_prompt (str): Prompt template for the LLM analysis
        """
        # Read the CSV file
        df = pd.read_csv(csv_path)

        # Add new column if it doesn't exist
        if new_column not in df.columns:
            df[new_column] = pd.Series(dtype="str")

        # Analyze each row
        for idx, row in df.iterrows():
            print(f"\n{'='*50}")
            print(f"Analyzing row {idx + 1}/{len(df)}")

            # Format the analysis prompt with row content
            prompt = analysis_prompt.format(content=row["content"])

            max_retries = 3
            retry_delay = 2

            for attempt in range(max_retries):
                try:
                    response = self.client.chat.completions.create(
                        model="deepseek-chat",
                        messages=[{"role": "user", "content": prompt}],
                        temperature=0.7,
                    )

                    # Get and clean the response
                    message_content = response.choices[0].message.content
                    cleaned_content = message_content.strip()

                    # Parse JSON response
                    try:
                        # Remove markdown code blocks if present
                        if cleaned_content.startswith("```json"):
                            cleaned_content = cleaned_content[7:]
                        if cleaned_content.endswith("```"):
                            cleaned_content = cleaned_content[:-3]
                        cleaned_content = cleaned_content.strip()

                        analysis = json.loads(cleaned_content)

                        # Format as text with feature, description pairs
                        formatted_text = []
                        for feature, description in analysis.items():
                            formatted_text.append(f"{feature}, {description}")

                        # Join with newlines and update dataframe
                        df.at[idx, new_column] = "\n".join(formatted_text)
                        print(f"Analysis result: {formatted_text[:200]}...")
                        break

                    except json.JSONDecodeError:
                        # If not JSON, store raw text
                        df.at[idx, new_column] = cleaned_content
                        print(f"Analysis result: {cleaned_content[:200]}...")
                        break

                except Exception as e:
                    if attempt < max_retries - 1:
                        print(f"Attempt {attempt + 1} failed: {str(e)}")
                        print(f"Retrying in {retry_delay} seconds...")
                        time.sleep(retry_delay)
                        retry_delay *= 2
                    else:
                        print(f"Failed after {max_retries} attempts: {str(e)}")
                        df.at[idx, new_column] = "Analysis failed"

        # Save the updated dataframe
        df.to_csv(csv_path, index=False)
        print(f"\nAnalysis complete. Results saved to {csv_path}")

    def analyze_dataframe(self, csv_path):
        # Read the CSV file
        df = pd.read_csv(csv_path)

        # Add scanned column if it doesn't exist
        if "scanned" not in df.columns:
            df["scanned"] = "N"

        # Add analysis columns if they don't exist with appropriate dtypes
        if "pain_points" not in df.columns:
            df["pain_points"] = pd.Series(dtype="str")
        if "gain_points" not in df.columns:
            df["gain_points"] = pd.Series(dtype="str")
        if "jobs_to_be_done" not in df.columns:
            df["jobs_to_be_done"] = pd.Series(dtype="str")
        if "themes" not in df.columns:
            df["themes"] = pd.Series(dtype="str")
        if "relevance_score" not in df.columns:
            df["relevance_score"] = pd.Series(dtype="float64")

        # Analyze each unscanned row
        for idx, row in df.iterrows():
            if row["scanned"] == "N":
                print(f"\n{'='*50}")
                print(f"Analyzing row {idx + 1}/{len(df)}")
                print(f"Content: {row['content'][:200]}...")
                analysis = self.analyze_post(row["content"])

                if analysis:
                    print("\nLLM Analysis Output:")
                    print(
                        json.dumps(
                            {
                                "pain_points": analysis["pain_points"],
                                "gain_points": analysis["gain_points"],
                                "jobs_to_be_done": analysis["jobs_to_be_done"],
                                "themes": analysis["themes"],
                                "relevance_score": analysis["relevance_score"],
                            },
                            indent=2,
                        )
                    )

                    df.at[idx, "pain_points"] = ", ".join(analysis["pain_points"])
                    df.at[idx, "gain_points"] = ", ".join(analysis["gain_points"])
                    df.at[idx, "jobs_to_be_done"] = ", ".join(
                        analysis["jobs_to_be_done"]
                    )
                    df.at[idx, "themes"] = ", ".join(analysis["themes"])
                    df.at[idx, "relevance_score"] = analysis["relevance_score"]
                else:
                    print(f"Analysis failed for row {idx + 1}")
                    df.at[idx, "pain_points"] = ""
                    df.at[idx, "gain_points"] = ""
                    df.at[idx, "jobs_to_be_done"] = ""
                    df.at[idx, "themes"] = ""
                    df.at[idx, "relevance_score"] = 0.1

                df.at[idx, "scanned"] = "Y"

                # Save progress after each analysis
                df.to_csv(csv_path, index=False)
            else:
                print(f"Skipping already analyzed row {idx + 1}")

        # Filter for relevance score >= 0.5
        filtered_df = df[df["relevance_score"] >= 0.5]

        # Save analyzed data with -staging suffix
        output_path = csv_path.rsplit(".", 1)[0] + "-staging.csv"
        filtered_df.to_csv(output_path, index=False)
        print(
            f"\nAnalysis complete. {len(filtered_df)} relevant entries saved to {output_path}"
        )
        return filtered_df


if __name__ == "__main__":
    analyzer = LLMAnalyzer()
    analyzer.create_analyzed_column(
        "analyzed_journaling_comments.csv",
        "ideal_features",
        """
        Based on the following Reddit comment, suggest ideal features for an AI journaling app that would address the user's needs and preferences.

        Content: {content}

        Format your response as a list of features with descriptions, like:
        Feature 1: Description 1
        Feature 2: Description 2
        etc.

        Focus on features that would:
        - Address pain points and challenges mentioned
        - Enhance the benefits and gains described
        - Help accomplish the jobs/tasks discussed
        - Align with the user's journaling style and preferences
        """,
    )
