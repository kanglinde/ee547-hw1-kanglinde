import sys
import json
import time
from datetime import datetime, timezone
import urllib.request as urlreq
import urllib.error as urlerr

TIMEOUT = 10

def GetTimeStamp():
	return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

def FetchUrl(url):
	res = {
		"url": url,
		"status_code": None,
		"response_time_ms": 0.0,
		"content_length": 0,
		"word_count": None,
		"timestamp": "",
		"error": None
	}

	start_time = time.time()
	def CalcTime():
		return (time.time() - start_time) * 1000
	
	try:
		with urlreq.urlopen(url, timeout=TIMEOUT) as response:
			res["response_time_ms"] = CalcTime()
			res["timestamp"] = GetTimeStamp()
			res["status_code"] = response.status
			content_type = response.headers.get("Content-Type", "")
			if "text" in content_type:
				content = response.read()
				res["content_length"] = len(content)
				res["word_count"] = len(content.decode("utf-8", errors="ignore").split())
	except urlerr.HTTPError as e:
		res["response_time_ms"] = CalcTime()
		res["timestamp"] = GetTimeStamp()
		res["status_code"] = e.code
		res["error"] = str(e)
	except Exception as e:
		res["response_time_ms"] = CalcTime()
		res["timestamp"] = GetTimeStamp()
		res["error"] = str(e)

	return res

def main():
	input_path = sys.argv[1]
	output_path = sys.argv[2]

	#Load input file
	urls = []
	with open(input_path) as file:
		for line in file:
			if len(line.strip()) > 0:	#in case there're empty lines
				urls.append(line.strip())

	#Start fetching
	responses = []
	summary = {
		"total_urls": 0,
		"successful_requests": 0,
		"failed_requests": 0,
		"average_response_time_ms": 0.0,
		"total_bytes_downloaded": 0,
		"status_code_distribution": {},
		"processing_start": GetTimeStamp()
	}
	errors = []

	total_response_time_ms = 0
	for url in urls:
		res = FetchUrl(url)
		responses.append(res)
		summary["total_urls"] += 1
		if res["error"] == None:
			summary["successful_requests"] += 1
		else:
			summary["failed_requests"] += 1
			errors.append(f'{res["timestamp"]} {res["url"]}: {res["error"]}')
		summary["total_bytes_downloaded"] += res["content_length"]
		if res["status_code"] != None:
			if str(res["status_code"]) in summary["status_code_distribution"]:
				summary["status_code_distribution"][str(res["status_code"])] += 1
			else:
				summary["status_code_distribution"][str(res["status_code"])] = 1
		total_response_time_ms += res["response_time_ms"]

	summary["average_response_time_ms"] = total_response_time_ms / summary["total_urls"]
	summary["processing_end"] = GetTimeStamp()

	#Write output files
	output_responses = f"{output_path}/responses.json"
	with open(output_responses, "w") as file:
		json.dump(responses, file, indent=2)

	output_summary = f"{output_path}/summary.json"
	with open(output_summary, "w") as file:
		json.dump(summary, file, indent=2)
	
	output_errors = f"{output_path}/errors.log"
	with open(output_errors, "w") as file:
		file.write("\n".join(errors))

main()