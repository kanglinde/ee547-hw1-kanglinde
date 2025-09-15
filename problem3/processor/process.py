import json
import os
import time
from datetime import datetime, timezone
import re

def strip_html(html_content):
    """Remove HTML tags and extract text."""
    # Remove script and style elements
    html_content = re.sub(r'<script[^>]*>.*?</script>', '', html_content, flags=re.DOTALL | re.IGNORECASE)
    html_content = re.sub(r'<style[^>]*>.*?</style>', '', html_content, flags=re.DOTALL | re.IGNORECASE)
    
    # Extract links before removing tags
    links = re.findall(r'href=[\'"]?([^\'" >]+)', html_content, flags=re.IGNORECASE)
    
    # Extract images
    images = re.findall(r'src=[\'"]?([^\'" >]+)', html_content, flags=re.IGNORECASE)
    
    # Remove HTML tags
    text = re.sub(r'<[^>]+>', ' ', html_content)
    
    # Clean whitespace
    text = re.sub(r'\s+', ' ', text).strip()
    
    return text, links, images

def TimeStamp():
	return datetime.now(timezone.utc).isoformat()

def AnalyzeText(text):
	result = {
		"word_count": 0,
        "sentence_count": 0,
        "paragraph_count": 0,
        "avg_word_length": 0.0
	}
	total_word_length = 0
	paragraphs = text.split('\n')
	for parag in paragraphs:
		parag = parag.strip()
		if len(parag) > 0:	# not empty parag
			result["paragraph_count"] += 1
			sentences = re.split(r'[.!?]+', parag)
			for sent in sentences:
				sent = sent.strip()
				if len(sent) > 0:	# not empty sentence
					result["sentence_count"] += 1
					for w in re.split(r'[,; ]+', sent):
						if len(w) > 0:	# not empty word
							result["word_count"] += 1
							total_word_length += len(w)
	result["avg_word_length"] = total_word_length / result["word_count"]
	return result

def main():
	print(f'[{TimeStamp()}] Processor starting', flush=True)

	# Wait for fetcher complete
	input_file = "/shared/status/fetch_complete.json"
	while not os.path.exists(input_file):
		print(f"Waiting for {input_file}...", flush=True)
		time.sleep(2)
	
	# Read HTMLs
	htmls = []
	with open(input_file, "r") as file:
		fetch_status = json.load(file)
	if "results" in fetch_status:
		for res in fetch_status["results"]:
			if ("file" in res) and (res["file"] is not None):
				htmls.append(res["file"])

	# Create output directory
	os.makedirs("/shared/processed", exist_ok=True)

	# Process data
	results = []
	src = "/shared/raw/"
	for html in htmls:
		output_file = f'/shared/processed/{html.replace(".html", ".json")}'
		res = {"html": html}
		try:
			print(f'Processing {html}...', flush=True)
			with open(f'{src}{html}', "r", encoding="utf-8") as file:
				html_content = file.read()

			text, links, images = strip_html(html_content)
			statistics = AnalyzeText(text)
			output = {
				"source_file": html,
				"text": text,
				"statistics": statistics,
				"links": links,
				"images": images,
				"processed_at": TimeStamp()
			}
			with open(output_file, "w") as file:
				json.dump(output, file, indent=2)
			res["file"] = html.replace(".html", ".json")
			res["status"] = "success"
			results.append(res)
		except Exception as e:
			res["file"] = None
			res["error"] = str(e)
			res["status"] = "failed"
			results.append(res)

	# Write completion status
	process_status = {
		"timestamp": TimeStamp(),
        "htmls_processed": len(htmls),
        "successful": sum(1 for r in results if r["status"] == "success"),
        "failed": sum(1 for r in results if r["status"] == "failed"),
        "results": results
	}

	with open("/shared/status/process_complete.json", "w") as file:
		json.dump(process_status, file, indent=2)

	print(f'[{TimeStamp()}] Processor complete', flush=True)

if __name__ == "__main__":
    main()