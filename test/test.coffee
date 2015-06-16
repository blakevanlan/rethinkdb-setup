require("mocha")
expect = require("chai").expect
r = require("rethinkdb")
Async = require("async")
RethinkdbSetup = require("../index")

describe "RethinkdbSetup", ->

   before (done) ->
      @connection = 
      r.connect {db: "test"}, (err, connection) =>
         return done(err) if err
         @connection = connection
         done()

   afterEach (done) ->
      Async.each ["table0", "table1"], (tableName, callback) =>
         # Ignore any errors.
         r.tableDrop(tableName).run @connection, -> callback()
      , done

   describe "setup", ->
      
      it "should create each table when it doesn't exist", (done) ->
         RethinkdbSetup.setup @connection, {tables: {table0: true, table1: true}}, (err) =>
            return done(err) if err
            r.tableList().run @connection, (err, tables) =>
               return done(err) if err
               expect(tables).to.contain("table0")
               expect(tables).to.contain("table1")
               done()

      it "should create a table with specified primary key", (done) ->
         config = {
            tables: {
               table0: "primaryKey"
            }
         }
         RethinkdbSetup.setup @connection, config, (err) =>
            return done(err) if err
            r.tableList().run @connection, (err, tables) =>
               return done(err) if err
               expect(tables).to.contain("table0")
               done()

      it "should create a table with specified secondary index", (done) ->
         config = {
            tables: {
               table0: ["id", "field0"]
            }
         }
         RethinkdbSetup.setup @connection, config, (err) =>
            return done(err) if err
            r.tableList().run @connection, (err, tables) =>
               return done(err) if err
               expect(tables).to.contain("table0")
               r.table("table0").indexList().run @connection, (err, indexes) ->
                  expect(indexes).to.contain("field0")
                  done()

   describe "empty", ->

      beforeEach (done) ->
         r.tableCreate("table0").run @connection, -> done()

      it "should empty the tables", (done) ->
         r.table("table0").insert({foo: "bar"}).run @connection, (err) =>
            return done(err) if err
            RethinkdbSetup.empty @connection, {tables: {table0:true}}, (err) =>
               return done(err) if err
               r.table("table0").run @connection, (err, cursor) ->
                  cursor.toArray (err, array) ->
                     expect(array).to.have.length(0)
                     done()

   describe "load", ->

      beforeEach (done) ->
         r.tableCreate("table0").run @connection, -> done()

      it "should load the data into the tables", (done) ->
         tables = {
            table0: {id: "123", foo: "bar"}
         }
         RethinkdbSetup.load @connection, tables, (err) =>
            return done(err) if err
            r.table("table0").get("123").run @connection, (err, data) =>
               expect(data.id).to.equal("123")
               expect(data.foo).to.equal("bar")
               done()


