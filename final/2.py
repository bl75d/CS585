import pymongo
import random
import string
import numpy as np
import time
from flask import Flask, render_template, request, Response, stream_with_context
app = Flask(__name__)

CUSTNUM = 500001
client = pymongo.MongoClient(host='localhost', port=27017)
db = client.test
collection = db.customers

@app.route('/')
def index():
    return render_template('sql.html')

def iterator(RANDOM_ORDER, number_passes, one_pass, inputkey, opername, oper, functioncolumn):
	# one pass
	if number_passes > 1:
		samples = list(RANDOM_ORDER[0:one_pass])
		result = collection.aggregate(
			[{'$match': {'ID':{'$in':samples}}},
			{'$group': {'_id': inputkey, opername:{oper: functioncolumn}}}]
		)
	else:
		result = collection.aggregate(
			[{'$group': {'_id': inputkey, opername:{oper: functioncolumn}}}]
		)
	return result

@stream_with_context
def SQL():
	line = ""
	function = request.values.get('function')
	calculate = request.values.get('calculate')
	column = request.values.get('column')
	param1 = request.values.get('percentage')
	param2 = request.values.get('threshold')
	log = request.values.get('log')
	if log == "logfalse":	log = ""
	line = column + " " + calculate + " " + function + " " + param1 + " " + param2
	SUM, AVG, CNT, MAX, MIN = 0, 0, 0, 0, 0

	percentage = 0
	threshold = 0
	keys = line.split()
	if keys[2].upper() == "SUM":	SUM = 1
	elif keys[2].upper() == "AVG":	AVG = 1
	elif keys[2].upper() == "COUNT":	CNT = 1
	elif keys[2].upper() == "MAX":	MAX = 1
	elif keys[2].upper() == "MIN":	MIN = 1
	else:
		print("input wrong!")
	if len(keys) > 3:
		percentage = float(keys[3])
		threshold = float(keys[4])
	else:
		percentage = 1
		threshold = 0

	# iterations
	record = dict()		#	[Key, appearences]
	difference = [CUSTNUM]

	one_pass = int((CUSTNUM-1) * percentage)
	number_passes = int(1 / percentage)
	RANDOM_ORDER = [i for i in range(1,CUSTNUM)]
	np.random.shuffle(RANDOM_ORDER)

	inputkey = "$" + keys[0]
	functioncolumn = "$" + keys[1]
	print("Group by:" + keys[0], "Calculate by:" + keys[1], "Percentage:" + str(percentage), "Threshold:" + str(threshold), "Samples per batch:" + str(one_pass))
	oper = "$" + keys[2].lower()
	opername = keys[2] + " " + keys[1]
	if CNT == 1:
		SUM = 1
		CNT = 0
		oper = "$sum"
		functioncolumn = 1

	iter = 0
	diff = sum(difference) / len(difference)
	while diff > threshold and iter < number_passes:
		iter += 1
		print("ITERATION " + str(iter))
		starttime = time.time()
		difference = []
		result = iterator(RANDOM_ORDER, number_passes, one_pass, inputkey, opername, oper, functioncolumn)
		RANDOM_ORDER = RANDOM_ORDER[one_pass:]
		for r in result:
			if r['_id'] not in record:
				if AVG == 1:
					record[r['_id']] = [r[opername], 1]
					difference.append(r[opername])
				elif SUM == 1:
					record[r['_id']] = [r[opername]/percentage, percentage]
					difference.append(r[opername])
				else:
					record[r['_id']] = [r[opername], 1]
					difference.append(r[opername])
			else:
				oldvalue = record[r['_id']][0]
				if AVG == 1:
					record[r['_id']][0] = record[r['_id']][0] * record[r['_id']][1] + r[opername]
					record[r['_id']][1] += 1
					record[r['_id']][0] /= record[r['_id']][1]
				elif SUM == 1:
					record[r['_id']][0] = record[r['_id']][0] * record[r['_id']][1] + r[opername]
					record[r['_id']][1] += percentage
					record[r['_id']][0] /= record[r['_id']][1]
				else:
					record[r['_id']][1] += 1
					if MAX == 1:
						record[r['_id']][0] = max(r[opername], record[r['_id']][0])
					elif MIN == 1:
						record[r['_id']][0] = min(r[opername], record[r['_id']][0])
				difference.append(abs(oldvalue - record[r['_id']][0]))
			# print(r['_id'], r[opername])
		diff = sum(difference)/len(difference)
		print("Diff: " + str(diff))
		yield render_template('temp.html',items=record,iter=iter,diff=diff,Key=inputkey[1:],Value=opername,Time=time.time()-starttime,Log=log)
	yield render_template('temp.html',items=record,iter="Result",diff=diff,Key=inputkey[1:],Value=opername,Log="True")

@app.route('/sql/',methods=['POST'])
def SQLrender():
	return Response(SQL())

if __name__ == '__main__':
	app.run(debug=True,host='127.0.0.1',port='8080')
