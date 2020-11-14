db.categories.drop()
db.categories.insertMany( [
    { _id: "MongoDB", parent: "Databases" },
    { _id: "dbm", parent: "Databases" },
    { _id: "Languages", parent: "Programming" },
    { _id: "Databases", parent: "Programming" },
    { _id: "Programming", parent: "Books" },
    { _id: "Books", parent: null }
 ] )
