//2(2)//check
db.test.aggregate([{$group:{_id:{year:{$year:"$birth"}},ids:{$push:{aid:"$_id"}}}}])