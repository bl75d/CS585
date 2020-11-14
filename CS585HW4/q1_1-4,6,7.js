//1(1)//check
db.test.insert({
    "_id" : 20,
    "name" : {
        "first" : "Alex", "last" : "Chen"
    },
    "birth" : ISODate("1933-08-27T04:00:00Z"),
    "death" : ISODate("1984-11-07T04:00:00Z"),
    "contribs" : [
        "C++", "Simula"
    ],
    "awards" : [
        { "award" : "WPI Award",
            "year" : 1977, "by" : "WPI"
        } ]
})

db.test.insert({
    "_id" : 30,
    "name" : {
        "first" : "David", "last" : "Mark"
    },
    "birth" : ISODate("1911-04-12T04:00:00Z"),
    "death" : ISODate("2000-11-07T04:00:00Z"),
    "contribs" : [
        "C++", "FP",
        "Lisp",
    ],
    "awards" : [ {
        "award" : "WPI Award", "year" : 1963,
        "by" : "WPI" },
        {
            "award" : "Turing Award",
            "year" : 1966, "by" : "ACM"
        } ]
})

//1(2)//check
db.test.find({$or:[{awards:{$exists:true},$where:'this.awards.length<3'},{ contribs:'FP'}]})
//1(3)//check
db.test.update({'name.first' : "Guido",'name.last': "van Rossum"},{$push:{contribs:'OOP'}})
//1(4)//check
db.test.update({'name.first':"Alex",'name.last':"Chan"},{$set:{'comments': ["He taught in 3 universities", "died from cancer", "lived in CA"]}})
//1(6)//check
db.test.distinct("awards.by")

//1(7)//check
db.test.remove({ 'awards.year':"2011"})