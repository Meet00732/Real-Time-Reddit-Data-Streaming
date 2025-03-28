from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import json
import os
import praw
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

default_args = {
    'owner': 'airmeet',
    'start_date': datetime(2025, 3, 27, 9, 0)
}

def init_reddit_client():
    """
    Initialize and return a Reddit API client.
    """
    return praw.Reddit(
        client_id=os.getenv("REDDIT_CLIENT_ID"),
        client_secret=os.getenv("REDDIT_CLIENT_SECRET"),
        user_agent=os.getenv("REDDIT_USER_AGENT")
    )

def get_comments_text(submission):
    """
    Retrieve and aggregate text from all comments of a submission.
    """
    # Replace "more comments" objects and flatten the comment tree
    submission.comments.replace_more(limit=0)
    comments = submission.comments.list()
    # Join comment bodies into a single text block
    return " ".join(comment.body for comment in comments if hasattr(comment, 'body'))


def get_reddit_data(reddit, subreddits, limit=1):
    """
    Retrieve a fixed number of past submissions from the specified subreddits.
    Here, we use the .new() method to get historical (previous) posts.
    """
    submissions = reddit.subreddit(subreddits).new(limit=limit)
    results = []
    for submission in submissions:
        text = f"{submission.title or ''}\n{submission.selftext or ''}"

        # If selftext is empty or very short, fetch comments for additional text
        if not submission.selftext or len(submission.selftext) < 200:
            comments_text = get_comments_text(submission)
            text = f"{submission.title or ''}\n{comments_text}".strip()
        else:
            comments_text = ""  # Optional: you can store comments separately if needed
        
        data = {
            "id": submission.id,
            "title": submission.title,
            "selftext": submission.selftext,
            "comment": comments_text,
            "subreddit": submission.subreddit.display_name,
            "created_utc": submission.created_utc,
            "score": submission.score,
            "upvote_ratio": submission.upvote_ratio,
            "num_comments": submission.num_comments,
            "url": submission.url,
            "domain": submission.domain,
            "author": str(submission.author),
            "text_length": len(text)
        }
        results.append(data)
    return results

def fetch_reddit_data(**kwargs):
    """
    Initializes the Reddit client, retrieves historical submissions from the
    'technology' subreddit, and writes the results to a JSON file.
    """
    reddit = init_reddit_client()
    subreddits = "chatgpt"  # Modify if needed or expand to multiple subreddits
    results = get_reddit_data(reddit, subreddits, limit=1)
    
    print(json.dumps(results, indent=3))
    
# with DAG(
#     dag_id='reddit_data_ingestion',
#     default_args=default_args,
#     schedule_interval='@daily',
#     catchup=False,
# ) as dag:
    
#     reddit_data_task = PythonOperator(
#         task_id='fetch_reddit_data',
#         python_callable=fetch_reddit_data,
#         provide_context=True,
#     )


fetch_reddit_data()