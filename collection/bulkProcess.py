import json
from datetime import datetime


def convert_date(utc_date):
	date_ = datetime.fromtimestamp(utc_date)
	date_ = str(date_)
	return date_


def get_post_title(link):
    link = link.split("/")
    title = link[-3]
    title = title.replace("_", " ")
    return title

def clean_other_id(idint):
    idint = idint.split("_")[1]
    return idint



def process_submissions():

	data = []

	with open("submissions.jsonl", "r") as f:
		for line in f:
			data.append(json.loads(line))

	processed_posts = []
	for i, submission in enumerate(data):
		# print(i)
		post = {
			"entry_type": "post",
			"title": submission["title"],
			"author": submission["author"],
			"created_utc": convert_date(submission["created_utc"]),
			"num_comments": submission["num_comments"],
			"body": submission["selftext"],
			"post_id": submission["id"],
			"subreddit": submission["subreddit"],
			"ups": submission.get("ups", 0),
			"downs": submission.get("downs", 0)
		}
		processed_posts.append(post)



	filename = "submissions_bulk.json"
	with open(filename, "w") as json_file:
		json.dump(processed_posts, json_file, indent=4)



def process_comments():

	comms = []

	with open("comments.jsonl", "r") as f:
		for line in f:
			comms.append(json.loads(line))


	processed_comments = []
	missed = 0
	for i, comment in enumerate(comms):
		try:
		# print(i)
			parent_id = comment["parent_id"]
			post_id = comment["link_id"]
		
			if parent_id == post_id:
				entry_type = "comment"
			else:
				entry_type = "reply"

			post_id = clean_other_id(comment["link_id"])
			parent_id = clean_other_id(comment["parent_id"])
			
			post = {
				"entry_type": entry_type,
				"title": get_post_title(comment["permalink"]),
				"author": comment["author"],
				"created_utc": convert_date(comment["created_utc"]),
				"body": comment["body"],
				"post_id": post_id,
				"subreddit": comment["subreddit"],
				"ups": comment.get("ups", 0),
				"downs": comment.get("downs", 0),
				"comment_id": comment["id"],
				"parent_id": parent_id
			}
			processed_comments.append(post)
		except:
			missed = missed + 1


	filename = "comments_bulk.json"
	with open(filename, "w") as json_file:
		json.dump(processed_comments, json_file, indent=4)




if __name__ == "__main__":
	process_submissions()
	process_comments()