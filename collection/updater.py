import psycopg2
import pandas as pd
import psycopg2.extras
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv
import json
import example
import process
import storage



load_dotenv()

db_params = {
	'dbname': os.getenv("dbname"),
	'user': os.getenv("user"),
	'password': os.getenv("password"),
	'host': os.getenv("host"),
	'port': os.getenv("port")
}



def get_past_date(how_back=10):
	now_date = datetime.now()
	upper_limit = now_date - timedelta(how_back)
	lower_limit = now_date - timedelta(2)
	return upper_limit, lower_limit



def query_docs():
	upper_limit, lower_limit = get_past_date(74)
	with psycopg2.connect(**db_params) as conn:
		cursor =  conn.cursor()
		sql_query = '''
					SELECT * FROM redposts
					WHERE created_utc BETWEEN %s AND %s
					'''
		cursor.execute(sql_query, (upper_limit, lower_limit))
		records = cursor.fetchall()
	return records



def develop_permalink(documents):
	developed_links = []
	for doc_ in documents:
		# print(doc_)
		post_id = doc_[0]
		subreddit = doc_[4]
		permalink = "/r/{}/comments/{}/".format(subreddit, post_id)
		developed_links.append(permalink)
	return developed_links


def update_database():
	docums = query_docs()
	permalinks = develop_permalink(docums)
	data_nested = example.scrape_individual_posts(permalinks)
	flattenned = process.seperate_data(data_nested)
	posts, comments = process.separate_posts_and_comments(flattenned)
	storage.insert_or_update_posts(posts)
	storage.insert_or_update_comments(comments)




if __name__ == "__main__":
	update_database()