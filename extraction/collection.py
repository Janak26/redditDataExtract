import pandas as pd
import psycopg2
from dotenv import load_dotenv
import os

load_dotenv()

db_params = {
	'dbname': os.getenv("dbname"),
	'user': os.getenv("user"),
	'password': os.getenv("password"),
	'host': os.getenv("host"),
	'port': os.getenv("port")
}


def connect_to_postgres():
	"""Connect to the PostgreSQL database server and return a connection object."""
	conn = None
	try:
		# Replace with your actual database credentials
		conn = psycopg2.connect(**db_params)
		print("Connection successful.")
		return conn
	except psycopg2.DatabaseError as error:
		print(f"Error connecting to the database: {error}")
		return None




def query_posts(query):
	connection = connect_to_postgres()
	"""Example of querying data."""
	results = {'post_id': [], 'author':[], 'body': [], 'created_utc': []}
	with connection.cursor() as cur:
		# res = cur.execute("SELECT * FROM redposts WHERE CREATED_UTC > '2026-03-10'")
		res = cur.execute(query)
		# print(res)
		for row in cur.fetchall():
			results['post_id'].append(row[0])
			results['author'].append(row[2])
			results['body'].append(row[3])
			results['created_utc'].append(row[5])
	connection.close()
	results = pd.DataFrame(results)
	return results



def query_comments(query):
	connection = connect_to_postgres()
	"""Example of querying data."""
	results = {'post_id': [], 'comment_id':[], 'parent_id':[], 'author':[], 'body': [], 'created_utc': []}
	with connection.cursor() as cur:
		# res = cur.execute("SELECT * FROM redcomments WHERE CREATED_UTC > '2026-03-10'")
		res = cur.execute(query)
		# print(res)
		for row in cur.fetchall():
			results['post_id'].append(row[1])
			results['comment_id'].append(row[0])
			results['parent_id'].append(row[2])
			results['author'].append(row[4])
			results['body'].append(row[5])
			results['created_utc'].append(row[7])
	connection.close()
	results = pd.DataFrame(results)
	return results




if __name__ == "__main__":
	query = "SELECT * FROM redcomments WHERE CREATED_UTC > '2026-03-10'"
	# res = query_posts(query)
	res = query_comments(query)
	print(res)