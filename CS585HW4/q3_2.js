function getChildren(id, l){
	var childrennodes = db.categories.find({parent: id});
	var i = 0;
	var currlevel = l;
	while (i < childrennodes.length()){
		var node = childrennodes[i]["_id"];
		var temp = getChildren(node, l+1);
		if (temp > currlevel)
			currlevel = temp;
		i += 1;
	}
	return currlevel;
}

var level = getChildren("Books", 1);
print(level);
