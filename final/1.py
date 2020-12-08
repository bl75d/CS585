import pymongo
import random
import string
from collections import defaultdict
from tqdm import tqdm

CUSTNUM = 500001
# TRANSNUM = 5000001

client = pymongo.MongoClient(host='localhost', port=27017)
db = client.test
collection = db.customers

for i in tqdm(range(1,CUSTNUM)):
	id = i
	name = ''.join(random.sample('zyxwvutsrqponmlkjihgfedcba',random.randint(10,20)))
	age = random.randint(10,70)
	gender = ''.join(random.sample(['male','female'],1))
	country = random.randint(1,10)
	salary = 9900 * random.random() + 100
	tempdic = {
		'ID': id,
		'Name': name,
		'Age': age,
		'Gender': gender,
		'CountryCode': country,
		'Salary': salary
	}
	collection.insert(tempdic)

