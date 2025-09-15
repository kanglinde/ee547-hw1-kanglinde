import sys
import json
import urllib.request as urlreq
import urllib.error as urlerr
import urllib.parse as urlpars
import xml.etree.ElementTree as ET
import time
from datetime import datetime, timezone
import re

TIMEOUT = 30

RETRY_CODE = 429

ARXIV = "http://export.arxiv.org/api/query"

ATOM = "http://www.w3.org/2005/Atom"

STOPWORDS = {'the', 'a', 'an', 'and', 'or', 'but', 'in', 'on', 'at', 'to', 'for',
             'of', 'with', 'by', 'from', 'up', 'about', 'into', 'through', 'during',
             'is', 'are', 'was', 'were', 'be', 'been', 'being', 'have', 'has', 'had',
             'do', 'does', 'did', 'will', 'would', 'could', 'should', 'may', 'might',
             'can', 'this', 'that', 'these', 'those', 'i', 'you', 'he', 'she', 'it',
             'we', 'they', 'what', 'which', 'who', 'when', 'where', 'why', 'how',
             'all', 'each', 'every', 'both', 'few', 'more', 'most', 'other', 'some',
             'such', 'as', 'also', 'very', 'too', 'only', 'so', 'than', 'not'}

#Outputs made Global for easier access
papers = []
analysis = {
   "query": "",
   "papers_processed": 0,
   "processing_timestamp": "",
   "corpus_stats": {
      "total_abstracts": 0,
      "total_words": 0,
      "unique_words_global": 0,
      "avg_abstract_length": 0.0,
      "longest_abstract_words": 0,
      "shortest_abstract_words": 0
   },
   "top_50_words": [],
   "technical_terms": {
      #set avoids duplicate, convert to list later
      "uppercase_terms": set(),
      "numeric_terms": set(),
      "hyphenated_terms": set()
   },
   "category_distribution": {}
}
process = []

def GetTimeStamp():
   return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

def Log(msg):
   process.append(f'{GetTimeStamp()} {msg}')

def QueryArxiv(search_query, max_results):
   #Generate url
   params = {
      "search_query": search_query,
      "start": 0,
      "max_results": max_results
   }
   url = f'{ARXIV}?{urlpars.urlencode(params).replace("%3A",":")}'

   #Start fetching
   status = ""
   result = None
   for i in range(3):
      try:
         with urlreq.urlopen(url, timeout=TIMEOUT) as response:
            xml_data = response.read()
            try:
               result = ET.fromstring(xml_data)
            except Exception as e:
               Log(f'Invalid XML: {str(e)}')
               result = None
            break
      except urlerr.HTTPError as e:
         status = str(e)
         if e.code == RETRY_CODE:
            time.sleep(3)
            continue
         break
      except Exception as e:
         status = str(e)
         break
      
   return status, result

def FindElem(root, elem):
   return root.find(f'{{{ATOM}}}{elem}')

def FindAllElem(root, elem):
   return root.findall(f'{{{ATOM}}}{elem}')

def ProduceAbstractStat(abstract):
   total_sentences = 0
   tot_word_length = 0
   sentences = re.split(r'[.!?]+', abstract.lower()) #lowercase to count case-insensitive
   words = []
   for sent in sentences:
      sent = sent.strip()
      if len(sent) > 0:
         total_sentences += 1
         for w in re.split(r'[,;\n ]+', sent):
            if len(w) > 0:
               words.append(w)
               tot_word_length += len(w)
   return {
      "total_words": len(words),
      "unique_words": len(set(words)),
      "total_sentences": total_sentences,
      "avg_words_per_sentence": len(words) / total_sentences,
      "avg_word_length": tot_word_length/len(words)
   }

def ProducePaper(entry):
   paper = {
      "arxiv_id": "",
      "title": "",
      "authors": [],
      "abstract": "",
      "categories": [],
      "published": "",
      "updated": "",
      "abstract_stats": {
         "total_words": 0,
         "unique_words": 0,
         "total_sentences": 0,
         "avg_words_per_sentence": 0.0,
         "avg_word_length": 0.0
      }
   }

   id = FindElem(entry, "id")
   if id is not None:
      full_id = id.text
      paper["arxiv_id"] = full_id.split("/")[-1]
   else:
      Log(f'Missing fields: arxiv_id')
   
   if paper["arxiv_id"] != "":
      Log(f'Processing paper: {paper["arxiv_id"]}')
   else:
      Log(f'Processing paper: unknown')

   title = FindElem(entry, "title")
   if title is not None:
      paper["title"] = title.text
   else:
      Log(f'Missing fields: title')

   authors = FindAllElem(entry, "author")
   if len(authors) == 0:
      Log(f'Missing fields: author')
   for author in authors:
      name = FindElem(author, "name")
      if name is not None:
         paper["authors"].append(name.text)
      else:
         Log(f'Missing fields: name')
      
   abstract = FindElem(entry, "summary")
   if abstract is not None:
      paper["abstract"] = abstract.text
      paper["abstract_stats"] = ProduceAbstractStat(paper["abstract"])
   else:
      Log(f'Missing fields: summary')

   categories = FindAllElem(entry, "category")
   if len(categories) == 0:
      Log(f'Missing fields: category')
   for cat in categories:
      paper["categories"].append(cat.get("term"))
   
   published = FindElem(entry, "published")
   if published is not None:
      paper["published"] = published.text
   else:
      Log(f'Missing fields: published')

   updated = FindElem(entry, "updated")
   if updated is not None:
      paper["updated"] = updated.text
   else:
      Log(f'Missing fields: updated')

   return paper

def ProduceOutput(output_path):
   #Convert to list (set is not json-compatible)
   analysis["technical_terms"]["uppercase_terms"] = list(analysis["technical_terms"]["uppercase_terms"])
   analysis["technical_terms"]["numeric_terms"] = list(analysis["technical_terms"]["numeric_terms"])
   analysis["technical_terms"]["hyphenated_terms"] = list(analysis["technical_terms"]["hyphenated_terms"])

   with open(f'{output_path}/papers.json', "w") as file:
      json.dump(papers, file, indent=2)

   with open(f'{output_path}/corpus_analysis.json', "w") as file:
      json.dump(analysis, file, indent=2)

   with open(f'{output_path}/processing.log', "w") as file:
      file.write("\n".join(process))

def FindTopFreq(words_freq, num):
   for i in range(num):
      if len(words_freq) == 0:
         break
      max_word = max(words_freq, key=lambda k: words_freq[k]["frequency"])
      max_data = words_freq.pop(max_word)
      max_data["documents"] = len(max_data["documents"])
      analysis["top_50_words"].append(max_data)

def main():
   search_query = sys.argv[1]
   max_results = sys.argv[2]
   output_path = sys.argv[3]

   #Query ArXiv
   Log(f'Starting ArXiv query: {search_query}')
   status, result = QueryArxiv(search_query, max_results)
   analysis["query"] = search_query

   #ArXiv unreachable
   if result == None:
      Log(f'Network error: {status}')
      ProduceOutput(output_path)
      sys.exit(1)

   #Parse result
   start_time = time.time()
   entries = FindAllElem(result, "entry")
   analysis["papers_processed"] = len(entries)
   Log(f'Fetched {len(entries)} results from ArXiv API')

   abstract_lengths = []
   unique_words = set()
   words_freq = {}

   corpus_status = analysis["corpus_stats"]
   technical_terms = analysis["technical_terms"]

   for entry in entries:
      paper = ProducePaper(entry)
      papers.append(paper)

      #Produce analysis
      corpus_status["total_abstracts"] += 1
      corpus_status["total_words"] += paper["abstract_stats"]["total_words"]
      abstract_lengths.append(paper["abstract_stats"]["total_words"])

      #Analyze abstract
      words = re.split(r'[,;.!?\n ]+', paper["abstract"])
      for w in words:
         if len(w) > 0:
            #Check uppercase
            if w == w.upper() and any(ch.isalpha() for ch in w) and w.lower() not in STOPWORDS:
               technical_terms["uppercase_terms"].add(w)
            #Check numeric
            if any(ch.isdigit() for ch in w):
               technical_terms["numeric_terms"].add(w)
            #Check hyphen
            if any(ch == '-' for ch in w):
               technical_terms["hyphenated_terms"].add(w)
            #Update unique words
            unique_words.add(w.lower())
            #Update words freq
            w_lower = w.lower()
            if w_lower not in STOPWORDS:
               if w_lower in words_freq:
                  words_freq[w_lower]["frequency"] += 1
                  words_freq[w_lower]["documents"].add(paper["arxiv_id"])
               else:
                  words_freq[w_lower] = {
                  "word": w,
                  "frequency": 1,
                  "documents": {paper["arxiv_id"]}
               }

      for cat in paper["categories"]:
         if cat in analysis["category_distribution"]:
            analysis["category_distribution"][cat] += 1
         else:
            analysis["category_distribution"][cat] = 1

   FindTopFreq(words_freq, 50)

   corpus_status["unique_words_global"] = len(unique_words)
   corpus_status["avg_abstract_length"] = corpus_status["total_words"] / corpus_status["total_abstracts"]
   corpus_status["longest_abstract_words"] = max(abstract_lengths)
   corpus_status["shortest_abstract_words"] = min(abstract_lengths)

   analysis["processing_timestamp"] = GetTimeStamp()

   Log(f'Completed processing: {len(entries)} papers in {time.time()-start_time} seconds')

   ProduceOutput(output_path)

main()