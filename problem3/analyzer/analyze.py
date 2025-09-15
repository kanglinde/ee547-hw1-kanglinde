import json
import os
import time
from datetime import datetime, timezone
import re

def jaccard_similarity(doc1_words, doc2_words):
    """Calculate Jaccard similarity between two documents."""
    set1 = set(doc1_words)
    set2 = set(doc2_words)
    intersection = set1.intersection(set2)
    union = set1.union(set2)
    return len(intersection) / len(union) if union else 0.0

def TimeStamp():
	return datetime.now(timezone.utc).isoformat()

def FindTopFreq(words_freq, num, total_words):
    top_words = []
    for i in range(num):
        if len(words_freq) == 0:
            break
        max_word = max(words_freq, key=lambda k: words_freq[k]["count"])
        max_data = words_freq.pop(max_word)
        top_words.append({
            "word": max_data["word"],
            "count": max_data["count"],
            "frequency": max_data["count"] / total_words
        })
    return top_words

def main():
    print(f'[{TimeStamp()}] Analyzer starting', flush=True)

    # Wait for processor complete
    input_file = "/shared/status/process_complete.json"
    while not os.path.exists(input_file):
        print(f"Waiting for {input_file}...", flush=True)
        time.sleep(2)

    # Read processed
    processed = []
    with open(input_file, "r") as file:
        process_status = json.load(file)
    if "results" in process_status:
        for res in process_status["results"]:
            if ("file" in res) and (res["file"] is not None):
                processed.append(res["file"])

    # Create output directory
    os.makedirs("/shared/analysis", exist_ok=True)

    # Produce statistics
    total_words = 0
    total_word_length = 0
    total_sentences = 0
    words_freq = {}
    docs_words = []
    bigrams = {}

    src = "/shared/processed/"
    for page in processed:
        print(f"Analyzing {page}...", flush=True)
        with open(f'{src}{page}', "r") as file:
            data = json.load(file)

        total_words += data["statistics"]["word_count"]
        total_word_length += data["statistics"]["word_count"] * data["statistics"]["avg_word_length"]
        total_sentences += data["statistics"]["sentence_count"]

        # collect words, bigrams(don't cross sentences)
        text = data["text"]
        words = []
        sentences = re.split(r'[,;.?!\n]+', text)
        for sent in sentences:
            sent = sent.strip()
            if len(sent) > 0:
                words_in_sent = []
                for w in sent.split():
                    if len(w) > 0:
                        words.append(w)
                        words_in_sent.append(w)
                        w_lower = w.lower()
                        if w_lower in words_freq:
                            words_freq[w_lower]["count"] += 1
                        else:
                            words_freq[w_lower] = {
                                "word": w,
                                "count": 1
                            }
                # collect bigrams
                if len(words_in_sent) > 1:  # must contains at least two words
                    for i in range(len(words_in_sent)-1):
                        big = f'{words_in_sent[i]} {words_in_sent[i+1]}'
                        if big in bigrams:
                            bigrams[big] += 1
                        else:
                            bigrams[big] = 1
        docs_words.append({
            "doc": page,
            "words": words
        })

    top_bigrams = []    # sorted
    while len(bigrams) > 0:
        max_bigram = max(bigrams, key=lambda k: bigrams[k])
        max_count = bigrams.pop(max_bigram)
        top_bigrams.append({
            "bigram": max_bigram,
            "count": max_count
        })

    unique_words = len(words_freq)
    top_100_words = FindTopFreq(words_freq, 100, total_words)

    document_similarity = []
    while len(docs_words) > 1:
        doc1 = docs_words.pop()
        for doc2 in docs_words:
            document_similarity.append({
                "doc1": doc1["doc"],
                "doc2": doc2["doc"],
                "similarity": jaccard_similarity(doc1["words"], doc2["words"])
            })

    avg_sentence_length = total_words / total_sentences
    avg_word_length = total_word_length / total_words

    # Save analysis
    result = {
        "processing_timestamp": TimeStamp(),
        "documents_processed": len(processed),
        "total_words": total_words,
        "unique_words": unique_words,
        "top_100_words": top_100_words,
        "document_similarity": document_similarity,
        "top_bigrams": top_bigrams,
        "readability": {
            "avg_sentence_length": avg_sentence_length,
            "avg_word_length": avg_word_length,
            "complexity_score": avg_word_length + avg_sentence_length
        }
    }

    with open("/shared/analysis/final_report.json", "w") as file:
        json.dump(result, file, indent=2)

    print(f'[{TimeStamp()}] Analyzer complete', flush=True)

    # Keep container running for a while to pass condition:
    #   "docker exec pipeline-analyzer test -f /shared/analysis/final_report.json"
    # which checked every 5 sec
    time.sleep(6)

if __name__ == "__main__":
    main()