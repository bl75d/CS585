//5
function contributionFetch(contribution){
	var people = db.test.find({contribs: contribution}, {_id:0, name: 1});
	var nameList = [];
	for (var i = 0; i < people.length(); i++){
		var tempname = people.toArray()[i].name;
		nameList.push(tempname);
	}
	return nameList;
}

var contributions = db.test.find({name: {first: "Alex", last: "Chen"}}, {contribs: 1, _id: 0}).toArray()[0].contribs;
var results = [];
for (var j = 0; j < contributions.length; j++){
	contributions.forEach(function(f){printjson(f)});
	var temp = {Contribution: contributions[j], People: contributionFetch(contributions[j])};
	results.push(temp);
}
results.forEach(function(f){printjson(f)});
