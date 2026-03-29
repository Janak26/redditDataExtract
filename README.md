![](https://raw.githubusercontent.com/Janak26/redditDataExtract/da2dbdcb967c79f7189cc646976e5d6dc9b834b1/arc.svg)


# Canada Express Entry Permenant Residency Timeline tracking using Reddit Data and Python

### Technologies used: Python (YARS, Pandas, PSYCOPG2, Regex), PostgreSQL, Ollama, OpenAI API, Qwen3.5:0.8B, Streamlit, Airflow

YARS: https://github.com/datavorous/yars

Reddit Bulk Data: https://academictorrents.com/details/3e3f64dee22dc304cdd2546254ca1f8e8ae542b4


### Data Collection:
- Modify the YARS open source code to collect the required fields from the subreddits.
- Parse the Reddit Bulk Data to extract the required subreddit data and store it in the PostgreSQL (just one time running)
- Run the collection - process.py to collect reddit posts at regular intervals
- Run the collection - updater.py every 10 days to update existing reddit posts with newer comments

### Data Extraction:
- Modify the SQL Query to select data in a particular timeframe
- Run head.py
- Collects posts and comments data from PostgreSQL
- Cleans the texts using Regex
- Have the option to chose OpenAI API (paid) or Qwen 3.5 0.8B (local, free)
- Model used for information Extraction from texts
- Extracted information parsed and validated
- Information Rearranged for easy analysis

### Possible Further improvements:
- Use Airflow to schedule tasks (currently using CRON)
- Retrain Qwen model with validated results obtained from Open AI API
- Streamlit to develop frontend
- Develop a timeline prediction model (Does not need ML, just simple statistics sufficient)
- Store procesed and structured timelines in the database
- Reduce OpenAPI calling by not querying posts which already have comlete timeline in the database
- Extract additional data - office of processing, in-Canada or out-of-Canada applicant, stream of draw (French category, CEC, helthcare, trade)
