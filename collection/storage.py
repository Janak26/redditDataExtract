import psycopg2
import pandas as pd
import psycopg2.extras
from datetime import datetime
import numpy as np
import time
import os
from dotenv import load_dotenv
import json



load_dotenv()

db_params = {
	'dbname': os.getenv("dbname"),
	'user': os.getenv("user"),
	'password': os.getenv("password"),
	'host': os.getenv("host"),
	'port': os.getenv("port")
}


def get_current_datetime():
	to_ret = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
	return to_ret


def convert_to_date(date_time):
	date_time = datetime.strptime(date_time, "%Y-%m-%d %H:%M:%S")
	return date_time



def create_tables():
	with psycopg2.connect(**db_params) as conn:
		cursor =  conn.cursor()
		sql_query = '''CREATE TABLE IF NOT EXISTS redPosts
				(
					post_id TEXT PRIMARY KEY,
					title TEXT,
					author TEXT,
					body TEXT,
					subreddit TEXT,
					created_utc TIMESTAMP,
					updated_date TIMESTAMP,
					num_comments INT,
					ups INT,
					downs INT,
					entry_type TEXT
					)'''
		cursor.execute(sql_query)
		sql_query = '''CREATE TABLE IF NOT EXISTS redComments
				(
					comment_id TEXT PRIMARY KEY,
					post_id TEXT REFERENCES redPosts(post_id),
					parent_id TEXT,
					title TEXT,
					author TEXT,
					body TEXT,
					subreddit TEXT,
					created_utc TIMESTAMP,
					ups INT,
					downs INT,
					entry_type TEXT,
					updated_date TIMESTAMP
					)'''
		cursor.execute(sql_query)
		conn.commit()



def insert_or_update_posts(posts):
	with psycopg2.connect(**db_params) as conn:
		cursor = conn.cursor()

		query = """
		INSERT INTO redposts (
			post_id, title, author, body, subreddit, created_utc, updated_date, num_comments, ups, downs, entry_type
		)
		VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
		ON CONFLICT (post_id)
		DO UPDATE SET
			title = EXCLUDED.title,
			author = EXCLUDED.author,
			body = EXCLUDED.body,
			subreddit = EXCLUDED.subreddit,
			created_utc = EXCLUDED.created_utc,
			updated_date = EXCLUDED.updated_date,
			num_comments = EXCLUDED.num_comments,
			ups = EXCLUDED.ups,
			downs = EXCLUDED.downs,
			entry_type = EXCLUDED.entry_type;
			
		"""

		for p in posts:

			post_id = p.get("post_id")
			title = p.get("title")
			author = p.get("author")
			body = p.get("body")
			subredit = p.get("subreddit")
			created_utc = p.get("created_utc")
			updated = get_current_datetime()
			num_comments = p.get("num_comments")
			ups = p.get("ups")
			downs = p.get("downs")
			entry_type = p.get("entry_type")

			docSingle = (post_id, title, author, body, subredit, created_utc, updated, num_comments, ups, downs, entry_type)

			# print(docSingle)

			cursor.execute(query, docSingle)


			conn.commit()
		cursor.close()



def insert_or_update_comments(comments):
	with psycopg2.connect(**db_params) as conn:
		cursor = conn.cursor()

		query = """
		INSERT INTO redcomments (
			comment_id, post_id, parent_id, title, author, body, subreddit, created_utc, ups, downs, entry_type, updated_date
		)
		VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
		ON CONFLICT (comment_id)
		DO UPDATE SET
			post_id = EXCLUDED.post_id,
			parent_id = EXCLUDED.parent_id,
			title = EXCLUDED.title,
			author = EXCLUDED.author,
			body = EXCLUDED.body,
			subreddit = EXCLUDED.subreddit,
			created_utc = EXCLUDED.created_utc,
			ups = EXCLUDED.ups,
			downs = EXCLUDED.downs,
			entry_type = EXCLUDED.entry_type,
			updated_date = EXCLUDED.updated_date;
		"""

		for c in comments:

			comment_id = c.get("comment_id")
			post_id = c.get("post_id")
			parent_id = c.get("parent_id")
			title = c.get("title")
			author = c.get("author")
			body = c.get("body")
			subredit = c.get("subreddit")
			created_utc = c.get("created_utc")
			ups = c.get("ups")
			downs = c.get("downs")
			entry_type = c.get("entry_type")
			updated = get_current_datetime()

			docSingle = (comment_id, post_id, parent_id, title, author, body, subredit, created_utc, ups, downs, entry_type, updated)


			cursor.execute(query, docSingle)


			conn.commit()
		cursor.close()



def load_json(file_path):
	with open(file_path, "r") as f:
		return json.load(f)



if __name__ == "__main__":
	# create_tables()
	
	postsPath = "posts.json"
	posts_data = load_json(postsPath)
	insert_or_update_posts(posts_data)

	commentsPath = "c_and_r.json"
	comments_data = load_json(commentsPath)
	insert_or_update_comments(comments_data)