//2(1)check
db.test.aggregate([
    {$unwind:"$awards"},
    {$group:{
        _id:"$awards.award",
        count:{"$sum":1}
    }
}])