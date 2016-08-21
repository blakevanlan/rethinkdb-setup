# RethinkDB Setup
Quickly sets up RethinkDB for testing.

[ ![Codeship Status for blakevanlan/rethinkdb-setup](https://codeship.com/projects/2f082350-e083-0133-44ac-7629dd68ac29/status?branch=master)](https://codeship.com/projects/145194)

## Install
```
npm install rethinkdb-setup
```

## Usage

Connect to a RethinkDB and set up tables.
```javascript
var RethinkdbSetup = require('rethinkdb-setup')

var config = {
   connection: {
      // Connection config, passed straight to r.connect.
      db: "test" // sets the default db
      host: "localhost" // sets the host
   },
   tables: {
      // Creates 'table0' with normal primary key.
      table0: true, 
      // Creates 'table1' with normal primary key.
      table1: "id", 
      // Creates 'table2' with 'primaryKey' as the primary key.
      table2: "primaryKey", 
      // Creates 'table3' with 'primaryKey' as the primary key and adds a
      // seconardy index on email.
      table3: ["primaryKey", "email"],
      // Creates 'table4' with normal primary key and adds a geo index to 'location'.
      table4: ["id", {name: "location", indexFunction: null, options: {geo: true}}],
      // Creates 'table5' with normal primary key and adds an index with an arbitrary
      // ReQL expression.
      table5: [
         "id", 
         {
            name: "location",
            indexFunction: function(user) {
               return r.add(user("last_name"), "_", user("first_name"));
            })
         }
      ]
   }
};

// NOTE: The rethinkdb module checks the validity of the connection using
// a instanceof meaning that the module can throw an error if two different
// versions are being used with the same connection.
RethinkdbSetup.connectAndSetup(config, function (err, connection) {
   //...
});

// If you already have a connnection, just pass it to setup directly.
RethinkdbSetup.setup(connection, config, function (err) {
   //...
});

```
Later on, you can then empty the tables.
```
RethinkdbSetup.empty(connection, config, function (err) {
   //...
});
```

You can also insert data into tables.
```javascript
data = {
   table0: [{id: '123', foo: 'bar'}, {id: '456', foo: 'apples'}]
};
RethinkdbSetup.load(rethinkdbConnection, data, function (err) {
   //...   
});
```
