//8
var result = [];
var aggres = db.test.aggregate(
	[{$unwind:"$awards"},{$match: {"awards.year":2001}},
	{$group: {_id: "$name", times:{$sum: 1}}},
	{$match:{times:{$gte:2}}}]
).toArray()
for (var i = 0; i < aggres.length; i++)
	result.push(aggres[i]._id);
result.forEach(function(f){printjson(f)});

//9
db.test.find({}).sort({_id:-1}).limit(1);

//10
db.test.findOne({"awards.by": "ACM"});
