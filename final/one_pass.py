import pymongo
import random
import string
from collections import defaultdict
from tqdm import tqdm
import numpy as np

CUSTNUM = 500001

client = pymongo.MongoClient(host='localhost', port=27017)
db = client.test
collection = db.customers

percentage = 0
threshold = 0
SUM = 0
AVG = 0
keys = []

# input: Grouping name, function column, Group function, percentage, threshold
line = raw_input()
keys = line.split()

if keys[2].upper() == "SUM":	SUM = 1
elif keys[2].upper() == "AVG":	AVG = 1
else:	print("input wrong!")
percentage = float(keys[3])
threshold = float(keys[4])

one_pass = int((CUSTNUM-1) * percentage)
number_passes = int(1 / percentage)

# one pass sample
print("Group by:" + keys[0], "Calculate by:" + keys[1], percentage, threshold, one_pass)
samples = list(np.random.randint(1,CUSTNUM,one_pass))
#result = collection.find({keys[0]:{'$in':samples}})

inputkey = "$" + keys[0]
fuctioncolumn = "$" + keys[1]
if SUM == 1:
	result = collection.aggregate(
		[{'$match': {'ID':{'$in':samples}}},
		{'$group': {'_id': inputkey, 'total ' + keys[1]:{'$sum': fuctioncolumn}}}]
	)
elif AVG == 1:
	result = collection.aggregate(
		[{'$match': {'ID':{'$in':samples}}},
		{'$group': {'_id': inputkey, 'average ' + keys[1]:{'$avg': fuctioncolumn}}}]
	)

for r in result:
	print(r)
