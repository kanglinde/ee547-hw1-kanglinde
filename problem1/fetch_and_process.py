import sys
import json
import time
import datetime
import os
import re
import urllib.request as urlreq

if (len(sys.argv) != 3):
	print("Error: Wrong number of arguments")
	exit(1)

input = sys.argv[1]
output = sys.argv[2]
print(input)
print(output)
