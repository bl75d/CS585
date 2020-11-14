//4(4)
function getDes(id,result) {
    var child = db.categories.findOne({_id: id}).children;
    var i=0;
    while (i<child.length){
        result.push(child[i]);
        result=getDes(child[i],result);
        i+=1;
    }
    return result;
}
var des=getDes("Books",[]);
print(des);