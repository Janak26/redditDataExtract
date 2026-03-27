import re
import json
import html
import pandas as pd




def process_text(text, window_size=6):
	# --- REMOVE SPECIFIC SPECIAL CHARACTERS ---
	special_chars_regex = re.compile(r"[\*\)\(\{\}🎉/~!🥳∙\\']")
	cleaned_text = special_chars_regex.sub('', text)
	text_lower = cleaned_text.lower()

	# --- KEYWORDS ---
	keyword_patterns = [
		# r'\bita\b', 
		r'\baor\b',
		r'\bbackground\b', r'\bbg verification\b', r'\bbg\b',
		r'\bbackground check\b', r'\bbackground checks\b', r'\bbg check\b',
		r'\beligibility\b', r'\belig\b',
		r'\bbio\b', r'\bbiometric\b', r'\bbiometrics\b', r'\bbiomet\b',
		r'\bbil\b',
		r'\bmedical\b', r'\bmedicals\b', r'\bmdeical\b',
		r'\bfinal decision\b', r'\bfd\b',
		r'\bp1\b', r'\bportal 1\b',
		r'\bp2\b', r'\bportal 2\b',
		r'\becopr\b'
	]
	keyword_regex = re.compile('|'.join(keyword_patterns), re.IGNORECASE)

	# --- DATE PATTERNS (INCLUDING COMPACT FORMATS LIKE Nov19 / Jan23) ---
	date_patterns = [
		r'\b\d{1,2}[./-]\d{1,2}\b',                         # 11.12
		r'\b\d{8}\b',                                      # 20260126
		r'\b\d{1,2}(st|nd|rd|th)?\b',                      # 6th
		r'\b(?:jan|feb|mar|apr|may|jun|jul|aug|sep|sept|oct|nov|dec)[a-z]*\d{1,2}\b',  # nov19
		r'\b\d{1,2}(?:jan|feb|mar|apr|may|jun|jul|aug|sep|sept|oct|nov|dec)[a-z]*\b'   # 19nov
	]
	date_regex = re.compile('|'.join(date_patterns), re.IGNORECASE)

	# --- Step 1: keyword must exist ---
	if not keyword_regex.search(text_lower):
		return 'invalid entry'

	# --- Step 2: validate proximity (RELAXED to support compact formats like Nov19) ---
	valid = False
	keyword_positions = []

	for kw_match in keyword_regex.finditer(text_lower):
		start = max(0, kw_match.start() - 30)
		end = min(len(text_lower), kw_match.end() + 30)
		window = text_lower[start:end]

		if date_regex.search(window):  # <-- key change: only date needed
			valid = True
			keyword_positions.append(kw_match.start())

	if not valid:
		return 'invalid entry'

	# --- Step 3: trim around keywords ---
	words = cleaned_text.split()
	word_spans = []
	idx = 0

	for i, word in enumerate(words):
		start = text_lower.find(word.lower(), idx)
		end = start + len(word)
		word_spans.append((start, end))
		idx = end

	selected_indices = set()

	for pos in keyword_positions:
		for i, (start, end) in enumerate(word_spans):
			if start <= pos < end:
				left = max(0, i - window_size)
				right = min(len(words), i + window_size + 1)
				selected_indices.update(range(left, right))
				break

	trimmed_words = [words[i] for i in sorted(selected_indices)]
	trimmed_text = ' '.join(trimmed_words)

	return trimmed_text




def validation(text):
	valid_keys = ["AOR", "BIL", "Medical", "Eligibility", "Background", "FD", "P1", "P2", "ECOPR"]

	# Use REGEX patterns instead of substring matching
	key_patterns = {
		r'\baor\b': "AOR",

		r'\bbil\b': "BIL",
		r'\bbiometric(s)?\b': "BIL",

		r'\bmedical(s)?\b': "Medical",

		r'\beligibility\b|\belig\b': "Eligibility",

		r'\bbackground\b|\bbg\b|\bbg check\b|\bbackground check\b|\bbackground verification\b': "Background",

		r'\bfinal decision\b|\bfd\b': "FD",

		r'\bp1\b|\bportal 1\b': "P1",
		r'\bp2\b|\bportal 2\b': "P2",

		r'\becopr\b|\bcopr\b': "ECOPR"
	}

	month_map = {
		"jan": "Jan", "feb": "Feb", "mar": "Mar", "apr": "Apr", "may": "May",
		"jun": "Jun", "jul": "Jul", "aug": "Aug", "sep": "Sep", "sept": "Sep",
		"oct": "Oct", "nov": "Nov", "dec": "Dec"
	}

	# --- Extract JSON ---
	json_match = re.search(r'\{.*?\}', text, re.DOTALL)
	if not json_match:
		return None

	json_str = json_match.group()
	json_str = html.unescape(json_str)
	json_str = json_str.replace(";", ":")
	json_str = re.sub(r'(\w+)\s*:', r'"\1":', json_str)
	json_str = json_str.replace("'", '"')

	try:
		data = json.loads(json_str)
	except:
		return None

	cleaned = {}

	def normalize_date(value):
		value = value.lower().strip()

		if value in ["", "null"] or not re.search(r'\d', value):
			return None

		# full + short month map
		month_map = {
			"january": "Jan", "jan": "Jan",
			"february": "Feb", "feb": "Feb",
			"march": "Mar", "mar": "Mar",
			"april": "Apr", "apr": "Apr",
			"may": "May",
			"june": "Jun", "jun": "Jun",
			"july": "Jul", "jul": "Jul",
			"august": "Aug", "aug": "Aug",
			"september": "Sep", "sep": "Sep", "sept": "Sep",
			"october": "Oct", "oct": "Oct",
			"november": "Nov", "nov": "Nov",
			"december": "Dec", "dec": "Dec"
		}

		month_pattern = r'(january|jan|february|feb|march|mar|april|apr|may|june|jun|july|jul|august|aug|september|sep|sept|october|oct|november|nov|december|dec)'

		# dd mmm / dd month
		m = re.search(rf'(\d{{1,2}})\s*{month_pattern}', value)
		if m:
			day = int(m.group(1))
			month = month_map[m.group(2)]
			return f"{day} {month}"
	
		# mmm dd / month dd
		m = re.search(rf'{month_pattern}\s*(\d{{1,2}})', value)
		if m:
			month = month_map[m.group(1)]
			day = int(m.group(2))
			return f"{day} {month}"

		return None

	# --- Process ---
	for k, v in data.items():
		k_clean = k.lower()

		# Split combined keys
		sub_keys = re.split(r'&|,|/', k_clean)

		for sub_key in sub_keys:
			sub_key = sub_key.strip()

			mapped_key = None
			for pattern, std_key in key_patterns.items():
				if re.search(pattern, sub_key):
					mapped_key = std_key
					break

			if not mapped_key:
				continue

			# ❗ DO NOT overwrite existing correct values
			if mapped_key in cleaned:
				continue

			norm_date = normalize_date(str(v))
			if not norm_date:
				continue

			cleaned[mapped_key] = norm_date

	if len(cleaned) < 1:
		return None

	return cleaned




if __name__ == "__main__":
	df = pd.read_csv('valid_full_comments_extracted_openai.csv')
	df['structured'] = df['chatgpt_extracted'].apply(validation)
	df.to_csv('valid_full_comments_extracted_openai.csv', index=False)