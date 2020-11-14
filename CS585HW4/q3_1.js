function getAncestor(id, ancestors){
	var parentnode = db.categories.findOne({_id: id}).parent;
	if (parentnode){
		var node = {Name: parentnode, Level: level};
		ancestors.push(node);
		level++;
		getAncestor(parentnode, ancestors);
	}
}

var ancestors = [];
var level = 1;
getAncestor("MongoDB", ancestors);
ancestors.forEach(function(f){printjson(f)});
