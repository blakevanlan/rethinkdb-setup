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

   describe "connectAndSetup", ->

      afterEach (done) ->
         r.dbList().run @connection, (err, list) =>
            if (list.indexOf("test2") != -1)
               r.dbDrop("test2").run @connection, (err) ->
                  done()
            else
               done()

      it "should create a connection", (done) ->
         config = {
            tables: {table0: true}
         }
         RethinkdbSetup.connectAndSetup config, (err, connection) ->
            return done(err) if err
            expect(connection).to.exist
            done()

      it "should create database if it doesn't exist", (done) ->
         config = {
            db: "test2"
            tables: {table0: true}
         }
         RethinkdbSetup.connectAndSetup config, (err, connection) ->
            return done(err) if err
            expect(connection).to.exist
            expect(connection.db).to.equal("test2")
            r.dbList().run connection, (err, list) ->
               return done(err) if err
               expect(list).to.contain("test2")
               done()

      it "should use the connection option if supplied", (done) ->
         config = {
            connection: {
               db: "test2"
            }
            tables: {table0: true}
         }
         RethinkdbSetup.connectAndSetup config, (err, connection) ->
            return done(err) if err
            expect(connection).to.exist
            expect(connection.db).to.equal("test2")
            r.dbList().run connection, (err, list) ->
               return done(err) if err
               expect(list).to.contain("test2")
               done()

   describe "setup", ->
      
      it "should create database if it doesn't exist", (done) ->
         r.connect {db: "test2"}, (err, connection) =>
            return done(err) if err
            RethinkdbSetup.setup connection, {tables: {table0: true}}, (err) =>
               return done(err) if err
               expect(connection.db).to.equal("test2")
               r.dbList().run connection, (err, list) ->
                  return done(err) if err
                  expect(list).to.contain("test2")
                  done()

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

      it "should create a table and secondary index with options", (done) ->
         config = {
            tables: {
               table0: ["id", {name: "field0", options: {multi: true}}]
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

      it "should create a table and secondary index with function", (done) ->
         config = {
            tables: {
               table0: [
                  "id",
                  {
                     name: "field0",
                     indexFunction: (row) -> return row("objects").map (obj) ->
                        return obj("id")
                  }
               ]
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

      it "should create a table and secondary index with function and options", (done) ->
         config = {
            tables: {
               table0: [
                  "id",
                  {
                     name: "field0",
                     options: {multi: true}
                     indexFunction: (row) -> return row("objects").map (obj) ->
                        return obj("id")
                  }
               ]
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

      it "should add secondary indexes to an existing table", (done) ->
         config1 = {
            tables: {
               table0: ["id", "field0"]
            }
         }
         config2 = {
            tables: {
               table0: ["id", "field0", "field1"]
            }
         }
         RethinkdbSetup.setup @connection, config1, (err) =>
            return done(err) if err
            r.tableList().run @connection, (err, tables) =>
               return done(err) if err
               expect(tables).to.contain("table0")
               r.table("table0").indexList().run @connection, (err, indexes) =>
                  return done(err) if err
                  expect(indexes).to.contain("field0")
                  expect(indexes).to.not.contain("field1")
                  RethinkdbSetup.setup @connection, config2, (err) =>
                     return done(err) if err
                     r.table("table0").indexList().run @connection, (err, indexes) =>
                        return done(err) if err
                        expect(indexes).to.contain("field1")
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
