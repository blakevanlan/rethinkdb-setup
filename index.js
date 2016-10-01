var insist = require('insist-types');
var r = require('rethinkdb');
var Async = require('async');

/**
 * Example config:
 * {
 *   connection: {
 *      db: "test",
 *      host: "localhost"
 *   },
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
   insist.args(arguments, Object, Function)
   if (!config.connection) {
      config.connection = {};
      if (config.host) {
         config.connection.host = config.host;  
      }
      if (config.db) {
         config.connection.db = config.db;
      }
   }
   if (!config.connection.db) {
      config.connection.db = 'test';
   }
   r.connect(config.connection, function (err, connection) {
      if (err) return callback(err);
      createDatabaseIfNeeded_(connection, function (err) {
         setup(connection, config, function (err) {
            if (err) return callback(err);
            callback(null, connection);
         });
      });
   });
};

var setup = function (connection, config, callback) {
   insist.args(arguments, Object, Object, Function)
   if (!config.tables) throw Error('Config object requires \'tables\' property'); 
   createDatabaseIfNeeded_(connection, function (err) {
      if (err) return callback(err);
      r.tableList().run(connection, function (err, tables) {
         if (err) return callback(err);
         Async.forEachOf(config.tables, function (value, tableName, done) {
            if (!value) {
               return done()
            }
            var primaryKey = getPrimaryKeyFromConfig_(value);
            var secondaryIndexes = getSecondaryIndexesFromConfig_(value);
            if (tables.indexOf(tableName) === -1) {
               // Create the table if it does not exist.
               r.tableCreate(tableName, {primaryKey: primaryKey}).run(connection, function (err) {
                  if (err) return done(err);
                  addSecondaryIndexes_(connection, tableName, secondaryIndexes, done);
               });
            } else {
               addSecondaryIndexes_(connection, tableName, secondaryIndexes, done);
            }
         }, callback);   
      });
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
   r.table(tableName).indexList().run(conn, function (err, currentIndexes) {
      if (err) return callback(err);
      Async.each(indexes, function (index, done) {
         var key = getSecondaryIndexKeyFromConfig_(index);
         if (currentIndexes.indexOf(key) === -1) {
            addSecondaryIndex_(conn, tableName, index, done);
         } else {
            done();
         }
      }, callback);
   });
};

var addSecondaryIndex_ = function (conn, tableName, index, callback) {
   insist.args(arguments, Object, String, [String, Object], Function)
   var key = getSecondaryIndexKeyFromConfig_(index);
   var createIndexQuery = null;
   var options = index.options || {};

   if (index.indexFunction) {
      createIndexQuery = r.table(tableName).indexCreate(key, index.indexFunction, options);
   } else {
      createIndexQuery = r.table(tableName).indexCreate(key, options);
   }
   createIndexQuery.run(conn, function () {
      r.table(tableName).indexWait(key).run(conn, callback);
   });
};

var createDatabaseIfNeeded_ = function (connection, callback) {
   insist.args(arguments, Object, Function)
   r.dbList().run(connection, function (err, list) {
      if (err) return callback(err);
      if (list.indexOf(connection.db) === -1) {
         r.dbCreate(connection.db).run(connection, function (err, data) {
            callback(err);
         });
      } else {
         callback();
      }
   });
};

var getPrimaryKeyFromConfig_ = function (tableConfig) {
   insist.args(arguments, [Boolean, String, Array])
   if (tableConfig === true || tableConfig === false) {
      return "id";
   }
   if (typeof tableConfig === "string") {
      return tableConfig;
   }
   // Must be an array of strings at this point.
   if (!(tableConfig instanceof Array)) {
      throw Error("value for tables must be a string or an array");
   }
   return tableConfig[0];
};

var getSecondaryIndexesFromConfig_ = function (tableConfig) {
   insist.args(arguments, [Boolean, String, Array])
   if (!(tableConfig instanceof Array)) {
      return [];
   }
   return tableConfig.splice(1); 
}

var getSecondaryIndexKeyFromConfig_ = function (indexConfig) {
   insist.args(arguments, [String, Object])
   if (typeof indexConfig === "string") {
      return indexConfig;
   }
   if (!indexConfig.name) {
      throw Error ("secondary index config missing name property:" + JSON.stringify(indexConfig));
   }
   return indexConfig.name;
}

module.exports = {
   connectAndSetup: connectAndSetup,
   setup: setup,
   empty: empty,
   load: load
};
