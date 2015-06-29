var insist = require('insist-types');
var r = require('rethinkdb');
var Async = require('async');

/**
 * Example config:
 * {
 *   db: "test"
 *   tables: {
 *      table0: true,
 *      table1: "id",
 *      table2: "primaryKey",
 *      table3: ["primaryKey", "email"],
 *      table4: ["id", {name: "location", indexFunction: null, options: {geo: true}}]
 *   }
 * }
 */

var connectAndSetup = function (config, callback) {
   r.connect({db: config.db || 'test'}, function (err, connection) {
      setup(connection, config, function (err) {
         if (err) return callback(err);
         callback(null, connection);
      });
   });
};

var setup = function (connection, config, callback) {
   insist.args(arguments, Object, Object, Function)
   if (!config.tables) throw Error('Config object requires \'tables\' property'); 

   r.tableList().run(connection, function (err, tables) {
      if (err) return callback(err);
      Async.forEachOf(config.tables, function (value, tableName, done) {
         // Only do anything if the table doesn't exist.
         if (tables.indexOf(tableName) === -1 && value) {
            if (value === true || value === "id") {
               return r.tableCreate(tableName).run(connection, done);
            }
            if (typeof value === "string") {
               return r.tableCreate(tableName, {primaryKey: value}).run(connection, done);
            }
            // Must be an array of strings at this point.
            if (!(value instanceof Array)) {
               throw Error("value for tables must be a string or an array");
            }
            var key = value.shift();
            r.tableCreate(tableName, {primaryKey: key}).run(connection, function (err) {
               if (err) return done(err);
               addSecondaryIndexes_(connection, tableName, value, done);
            });
         } else {
            done();
         }
      }, callback);   
   });
};

var empty = function (connection, config, callback) {
   insist.args(arguments, Object, Object, Function)
   if (!config.tables) throw Error('Config object requires \'tables\' property'); 
   
   Async.forEachOf(config.tables, function (value, tableName, done) {
      r.table(tableName).delete().run(connection, done)
   }, callback);
};

var load = function (connection, tables, callback) {
   insist.args(arguments, Object, Object, Function)
   Async.forEachOf(tables, function (value, tableName, done) {
      r.table(tableName).insert(value).run(connection, done)
   }, callback);
}  

var addSecondaryIndexes_ = function (conn, tableName, indexes, callback) {
   insist.args(arguments, Object, String, insist.arrayOf([String, Object]), Function)
   Async.each(indexes, function (index, done) {
      if (typeof index === "string" || (!index.indexFunction && !index.options)) {
         var key = (typeof index === "string") ? index : index.name
         return r.table(tableName).indexCreate(key).run(conn, done);
      }
      var options = index.options || {};
      if (index.indexFunction) {
         r.table(tableName).indexCreate(index.name, index.indexFunction, options).run(conn, done);   
      } else {
         r.table(tableName).indexCreate(index.name, options).run(conn, done);
      }
   }, callback);
};

module.exports = {
   connectAndSetup: connectAndSetup,
   setup: setup,
   empty: empty,
   load: load
};
