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


# # Evaluation and Analysis
# if keys[2].upper() == "COUNT":
# 	errorrate,errors=count_evaluation(record, inputkey[1:])
# elif keys[2].upper() == "SUM":
# 	errorrate,errors = sum_evaluation(record,keys[1],inputkey[1:])
# elif keys[2].upper() == "AVG":
# 	errorrate,errors = avg_evaluation(record,keys[1],inputkey[1:])
def count_evaluation(record,groupby):
	keycolumn='$'+groupby
	result = collection.aggregate(
		[{'$group': {'_id': keycolumn,'cnt': {'$sum': 1}}}]
	)
	error=[]
	errors=0
	for a in result:
		index=a['_id']
		error.append((a['cnt']-record[index][0])/a['cnt'])
		errors+=abs(a['cnt']-record[index][0])
	errorrate=sum(error)/len(error)
	return errorrate,errors

def sum_evaluation(record,select,groupby):
	keycolumn='$'+groupby
	selectcolumn='$'+select
	result = collection.aggregate(
		[{'$group': {'_id': keycolumn,'sum': {'$sum': selectcolumn}}}]
	)
	error=[]
	errors=0
	for a in result:
		# print(a)
		index=a['_id']
		error.append((a['sum']-record[index][0])/a['sum'])
		errors+=abs(a['sum']-record[index][0])
	errorrate = sum(error) / len(error)
	return errorrate,errors

def avg_evaluation(record,select,groupby):
	keycolumn='$'+groupby
	selectcolumn='$'+select
	result = collection.aggregate(
		[{'$group': {'_id': keycolumn,'avg': {'$avg': selectcolumn}}}]
	)
	error=[]
	errors=0
	for a in result:
		# print(a)
		index=a['_id']
		error.append((a['avg']-record[index][0])/a['avg'])
		errors+=abs(a['avg']-record[index][0])
	errorrate = sum(error) / len(error)
	return errorrate, errors