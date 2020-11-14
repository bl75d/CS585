//3(3)
db.categories.drop()
db.categories.insertMany( [
    { _id: "MongoDB", children:[]},
    { _id: "dbm", children:[] },
    { _id: "Languages", children:[] },
    { _id: "Databases", children:["MongoDB","dbm"] },
    { _id: "Programming", children:["Databases","Languages"] },
    { _id: "Books", children:["Programming"] }
] )

db.categories.find({children:"dbm"},{_id:1})