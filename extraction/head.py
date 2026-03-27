import collection
import cleaning
import model
import pandas as pd
from datetime import datetime, date
import ast
from dotenv import load_dotenv
import os


load_dotenv()

today_date = str(date.today())



posts_filename = os.getenv("posts_filename")
comments_filename = os.getenv("comments_filename")
final_filename = os.getenv("final_filename")
postsAllExtractedFilename = os.getenv("postsAllExtractedFilename")
commentsAllExtractedFilename = os.getenv("commentsAllExtractedFilename")

posts_filename = posts_filename.format(today_date)
comments_filename = comments_filename.format(today_date)
final_filename = final_filename.format(today_date)
postsAllExtractedFilename = postsAllExtractedFilename.format(today_date)
commentsAllExtractedFilename = commentsAllExtractedFilename.format(today_date)


def data_collection(query, category):
	if category == 'comments':
		# query = "SELECT * FROM redcomments WHERE CREATED_UTC > '2025-08-01'"
		data = collection.query_comments(query)
		data.to_csv(comments_filename, index=False)
		return data
	
	if category == 'posts':
		# query = "SELECT * FROM redposts WHERE CREATED_UTC > '2025-08-01'"
		data = collection.query_posts(query)
		data.to_csv(posts_filename, index=False)
		return data
	
	



def structured_data_extraction(text, model_type):
	if model_type == "openai":
		extractedText = model.perform_extraction_openai(text)
		return extractedText    
	
	if model_type == "qwen":
		extractedText = model.perform_extraction_openai(text)
		return extractedText
	
	return
	

def data_validation(text):
	structured_text = cleaning.validation(text)
	return structured_text



def convert_to_dict(text):
	if type(text) == dict:
		return text
	text = ast.literal_eval(text)
	return text



def structure_concated(data):
	dlist = []
	for index, row in data.iterrows():
		initial = {}
		initial['author'] = row['author']
		initial['created_utc'] = row['created_utc']
		more_dicts = convert_to_dict(row['structured'])
		initial.update(more_dicts)
		dlist.append(initial)
	processed = pd.DataFrame(dlist)
	return processed




def final_arrangement(posts, comments):
	concated = pd.concat([posts, comments])
	concated = structure_concated(concated)

	concated = concated[['author', 'created_utc', 'AOR', 'BIL', 'Medical', 'Eligibility', 'Background', 'FD', 'P1', 'P2', 'ECOPR']]

	grouped = concated.groupby(by=['author']).max()
	grouped = concated.reset_index(drop=False)
	grouped = grouped.sort_values(by=['created_utc'], ascending=True)
	grouped = grouped[['author', 'AOR', 'BIL', 'Medical', 'Eligibility', 'Background', 'FD', 'P1', 'P2', 'ECOPR']]
	grouped.to_csv(final_filename, index=False)



def full_process(posts_collection_query, comments_collection_query, extraction_model="qwen"):
	postsData = data_collection(posts_collection_query, 'posts')
	commentsData = data_collection(comments_collection_query, 'comments')
	print("Finished Data Collection")


	if len(postsData) > 0:
		postsData['cleaned_body'] = postsData['body'].apply(cleaning.process_text)
		postsDataRelevant = postsData.loc[postsData['cleaned_body'] != 'invalid entry']
	else:
		print("No Relevant Post Data")	


	if len(commentsData) > 0:
		commentsData['cleaned_body'] = commentsData['body'].apply(cleaning.process_text)
		commentsDataRelevant = commentsData.loc[commentsData['cleaned_body'] != 'invalid entry']
	else:
		print("No Relevant Comment Data")	
	print("Finished Data Cleaning")



	print("Starting LLM calling for comments")

	if len(commentsDataRelevant) > 0:
		commentsDataRelevant['information'] = commentsDataRelevant.apply(lambda x: structured_data_extraction(x['cleaned_body'], extraction_model), axis=1)
		commentsDataRelevant.to_csv(commentsAllExtractedFilename, index=False)

		commentsDataRelevant['structured'] = commentsDataRelevant['information'].apply(cleaning.validation)
		commentsDataRelevant = commentsDataRelevant.loc[pd.notnull(commentsDataRelevant['structured'])]
		commentsDataRelevant = commentsDataRelevant[['author', 'created_utc', 'structured']]
	else:
		print("No relevant posts after cleaning")


	print("Starting LLM calling for posts")

	if len(postsDataRelevant) > 0:
		postsDataRelevant['information'] = postsDataRelevant.apply(lambda x: structured_data_extraction(x['cleaned_body'], extraction_model), axis=1)
		postsDataRelevant.to_csv(postsAllExtractedFilename, index=False)
		
		postsDataRelevant['structured'] = postsDataRelevant['information'].apply(cleaning.validation)
		postsDataRelevant = postsDataRelevant.loc[pd.notnull(postsDataRelevant['structured'])]
		postsDataRelevant = postsDataRelevant[['author', 'created_utc', 'structured']]
	else:
		print("No relevant comments after cleaning")
	
	
	if (len(postsDataRelevant) > 0) or (len(commentsDataRelevant) > 0):
		final_arrangement(postsDataRelevant, commentsDataRelevant)



if __name__ == "__main__":
	llm = "openai"
	postsquery = "SELECT * FROM redposts WHERE CREATED_UTC > '2026-03-27'"
	commentsquery = "SELECT * FROM redcomments WHERE CREATED_UTC > '2026-03-27'"

	full_process(postsquery, commentsquery, llm)