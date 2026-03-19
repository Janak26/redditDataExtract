import json
from datetime import datetime
import example
import storage


def convert_date(utc_date):
	if type(utc_date) == str:
		utc_date = int(utc_date)
	date_ = datetime.fromtimestamp(utc_date)
	date_ = str(date_)
	return date_



def clean_parent_and_post_id(idint):
    idint = idint.split("_")[1]
    return idint



def flatten_replies(replies, post_title, subreddit, dataset):
	for reply in replies:
		dataset.append({
						"entry_type": "reply",
						"title": post_title,
						"author": reply["author"],
						"created_utc": convert_date(reply["created_utc"]),
						"body": reply["body"],
						"comment_id": reply["id"],
						"parent_id": clean_parent_and_post_id(reply["parent_id"]),
						"post_id": clean_parent_and_post_id(reply["link_id"]),
						"subreddit": subreddit,
						"ups": reply["ups"],
						"downs": reply["downs"]
						})

		if reply.get("replies"):
			flatten_replies(reply["replies"], post_title, subreddit, dataset)



def seperate_data(full_post):
	dataset = []

	for entry in full_post:
		# print('Entry  ', entry['created_utc'])		
		post_info = {
			"entry_type": "post",
			"title": entry["title"],
			"author": entry["author"],
			"created_utc": convert_date(entry["created_utc"]),
			"num_comments": entry["num_comments"],
			"body": entry["body"],
			"post_id": entry["id"],
			"subreddit": entry["subreddit"],
			"ups": entry["ups"],
			"downs": entry["downs"]
		}
		
		dataset.append(post_info)

		post_comments = entry.get("comments", [])
		
		for comment in post_comments:
			# print("Comment ", comment["created_utc"])
			dataset.append({
						"entry_type": "comment",
						"title": entry["title"],
						"author": comment["author"],
						"created_utc": convert_date(comment["created_utc"]),
						"body": comment["body"],
						"comment_id": comment["id"],
						"parent_id":  clean_parent_and_post_id(comment["parent_id"]),
						"post_id": clean_parent_and_post_id(comment["link_id"]),
						"subreddit": entry["subreddit"],
						"ups": comment["ups"],
						"downs": comment["downs"]
						})
			
			if comment.get("replies"):
				flatten_replies(comment["replies"], entry["title"], entry["subreddit"], dataset)
	
	return dataset



def save_json_file(reddit_data, subreddit_name):
	filename = "/home/linux/linux/ProjectsReal/redditEE/data/{}.json".format(subreddit_name)
	with open(filename, "w") as json_file:
		json.dump(reddit_data, json_file, indent=4)




def collect_subreddit_data(subreddit_name):
	collected_data = example.scrape_subreddit_data(subreddit_name, limit=15)
	# save_json_file(collected_data, "mumbai_test")
	collected_data = seperate_data(collected_data)
	return collected_data
	# save_json_file(collected_data, subreddit_name)



def collect_subrreddit_data_from_permalinks(permalinks):
	collected_individual_posts = example.scrape_individual_posts(permalinks)
	collected_individual_posts = seperate_data(collected_individual_posts)
	return collected_individual_posts
	# save_json_file(collected_individual_posts, "random_test")




def separate_posts_and_comments(all_data):
	posts = []
	comment_and_replies = []
	for one_data in all_data:
		if one_data["entry_type"] == "post":
			posts.append(one_data)
		else:
			comment_and_replies.append(one_data)
	# save_json_file(posts, "posts")
	# save_json_file(comment_and_replies, "c_and_r")
	return posts, comment_and_replies




def run_collection_pipeline(subreddits):
	for subreddit in subreddits:
		print("Collecting: ", subreddit)
		flattenned = collect_subreddit_data(subreddit)
		# flattenned = seperate_data(data_nested)
		posts, comments = separate_posts_and_comments(flattenned)
		storage.insert_or_update_posts(posts)
		storage.insert_or_update_comments(comments)



if __name__ == "__main__":
	# with open("subreddit_data.json", "r") as file:
	# 	data = json.load(file)
	# 	res = seperate_data(data)

	# filename = "subreddit_data_proessed.json"
	# with open(filename, "w") as json_file:
	# 	json.dump(res, json_file, indent=4)

	# print(res)
	# collect_subreddit_data('AskReddit')

	# plist = ["/r/AskReddit/comments/1rxf9tq/what_job_pays_surprisingly_well_but_nobody_talks/", "/r/AskReddit/comments/1rxv1s5/what_do_you_think_about_when_youre_walking/"]
	# c_data = collect_subrreddit_data_from_permalinks(plist)
	# separate_posts_and_comments(c_data)

	slist = ["AskReddit"]

	run_collection_pipeline(slist)