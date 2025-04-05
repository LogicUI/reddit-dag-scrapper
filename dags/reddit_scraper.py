import praw
import pandas as pd
from datetime import datetime
import os
from dotenv import load_dotenv
from requests.exceptions import ReadTimeout
from prawcore.exceptions import RequestException
import hashlib

load_dotenv()


class RedditScraper:
    def __init__(self):
        client_id = os.getenv("REDDIT_CLIENT_ID")
        client_secret = os.getenv("REDDIT_CLIENT_SECRET")
        username = os.getenv("REDDIT_USERNAME")
        password = os.getenv("REDDIT_PASSWORD")

        self.reddit = praw.Reddit(
            client_id=client_id,
            client_secret=client_secret,
            username=username,
            timeout=100,
            password=password,
            user_agent="script:reddit_scraper:v1.0 (by /u/ClassicStruggle6185)",
        )

        # Test authentication
        try:
            print("Testing authentication...")
            print(f"Read-only: {self.reddit.read_only}")

            print(f"User agent: {self.reddit.config.user_agent}")
            # Try to access something simple
            print("Attempting to access Reddit...")
            self.reddit.user.me()
            print("Authentication successful!")
        except Exception as e:
            print(f"Authentication failed: {str(e)}")

    def generate_hash(self, text):
        """Generate SHA-256 hash of text"""
        return hashlib.sha256(text.encode("utf-8")).hexdigest()

    def check_duplicates(self, df, new_comments):
        """Check for duplicates using comment hashes"""
        if df.empty:
            return new_comments

        existing_hashes = set(df["comment_hash"].values)
        return [
            comment
            for comment in new_comments
            if comment["comment_hash"] not in existing_hashes
        ]

    def scrape_subreddit_comments(self, subreddit_name, hot_limit=10, new_limit=25):
        """Scrape top comments from both hot and new posts with different limits"""
        subreddit = self.reddit.subreddit(subreddit_name)
        comments = []

        try:
            # Scrape with different limits for hot and new
            submission_streams = [
                ("hot", subreddit.hot(limit=hot_limit)),
                ("new", subreddit.new(limit=new_limit)),
            ]

            for post_type, submission_stream in submission_streams:
                for submission in submission_stream:
                    try:
                        submission.comments.replace_more(limit=0)

                        # Generate hash for post
                        post_hash = self.generate_hash(
                            submission.title + submission.selftext
                        )

                        for comment in submission.comments.list():
                            # Skip AutoModerator
                            if comment.author == "AutoModerator":
                                continue

                            # Generate hash for comment
                            comment_hash = self.generate_hash(comment.body)

                            comments.append(
                                {
                                    "id": comment.id,
                                    "content": comment.body,
                                    "author": (
                                        comment.author.name
                                        if comment.author
                                        else "deleted"
                                    ),
                                    "created_utc": datetime.fromtimestamp(
                                        comment.created_utc
                                    ),
                                    "score": comment.score,
                                    "permalink": f"https://reddit.com{comment.permalink}",
                                    "subreddit": subreddit_name,
                                    "parent_id": comment.parent_id,
                                    "is_submitter": comment.is_submitter,
                                    "post_title": submission.title,
                                    "post_hash": post_hash,
                                    "comment_hash": comment_hash,
                                    "post_type": post_type,
                                }
                            )

                    except Exception as e:
                        print(f"Error processing submission {submission.id}: {str(e)}")
                        continue

        except Exception as e:
            print(f"Error occurred while scraping comments: {str(e)}")
            raise

        return comments


def main():
    scraper = RedditScraper()
    subreddit_name = "journaling"


    hot_limit = 10  # Top 10 hot posts
    new_limit = 25  # Top 25 new posts since these rotate more frequently

    try:
        # Load existing data if available
        output_file = f"reddit_comments_{subreddit_name}.csv"
        try:
            existing_df = pd.read_csv(output_file)
        except FileNotFoundError:
            existing_df = pd.DataFrame()

        # Scrape new comments with adjusted limits
        print(f"Scraping comments from r/{subreddit_name}...")
        new_comments = scraper.scrape_subreddit_comments(
            subreddit_name, hot_limit=hot_limit, new_limit=new_limit
        )

        # Check for duplicates
        unique_comments = scraper.check_duplicates(existing_df, new_comments)

        if unique_comments:
            # Create DataFrame with new unique comments
            new_df = pd.DataFrame(unique_comments)

            # Combine with existing data
            if not existing_df.empty:
                df = pd.concat([existing_df, new_df], ignore_index=True)
            else:
                df = new_df

            # Save to CSV
            df.to_csv(output_file, index=False)
            print(f"\nResults saved to {output_file}")
            print(f"Added {len(unique_comments)} new comments")
        else:
            print("No new comments to add")

    except Exception as e:
        print(f"Error occurred: {str(e)}")


if __name__ == "__main__":
    main()
