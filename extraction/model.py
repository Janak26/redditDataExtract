import ollama
import pandas as pd
import json
from openai import OpenAI
from dotenv import load_dotenv


load_dotenv()

client = OpenAI()


with open("newsimple.txt", "r") as f:
	knowledge_base_qwen = f.read()


def perform_extraction_qwen(texts):
	# --- load knowledge base once ---
	
	
	system_message = {
	"role": "system",
	"content": f"""
	You are an expert at extracting Canadian Express Entry timelines.

	Reference knowledge:
	{knowledge_base_qwen}
	"""
	}

	# results = []

	# for reddit_text in reddit_texts:
	response = ollama.chat(
				model="qwen3.5:0.8b",
				think=False,
				messages=[
					system_message,
						{
					"role": "user",
					"content": f"""
					Extract timeline events from this text.
					Text:{texts}
					Return JSON."""
						}
					])
	
	output = response['message']['content']
		# results.append(output)
	return output



KNOWLEDGE_BASE_OPENAI = """
You are an expert at extracting Canadian Express Entry timelines.

Important rules:
- Extract only these fields:
  AOR, BIL, Medical, Eligibility, Background, FD, P1, P2, ECOPR
- Value MUST be a date in format: "dd Mmm" (e.g., 4 Nov)
- Convert formats like:
  - Nov 4 → 4 Nov
  - 20260126 → 26 Jan
  - Jan23 → 23 Jan
- Ignore any field without a valid date"""



def perform_extraction_openai(text):
	response = client.chat.completions.create(
		model="gpt-4.1-mini",  # fast + cheap + good for extraction
		temperature=0,
		messages=[
			{
				"role": "system",
				"content": KNOWLEDGE_BASE_OPENAI
			},
			{
				"role": "user",
				"content": f"Extract timeline from:\n{text}"
			}
		],
		response_format={
			"type": "json_schema",
			"json_schema": {
				"name": "timeline_extraction",
				"schema": {
					"type": "object",
					"properties": {
						"AOR": {"type": "string"},
						"BIL": {"type": "string"},
						"Medical": {"type": "string"},
						"Eligibility": {"type": "string"},
						"Background": {"type": "string"},
						"FD": {"type": "string"},
						"P1": {"type": "string"},
						"P2": {"type": "string"},
						"ECOPR": {"type": "string"}
					},
					"additionalProperties": False
				}
			}
		}
	)

	result = response.choices[0].message.content

	if result:
		result = str(result)
		return result
		# return json.loads(result)
	return None




if __name__ == "__main__":
	rand = "Insert some random string"
	res = perform_extraction_qwen(rand)
	print(res)