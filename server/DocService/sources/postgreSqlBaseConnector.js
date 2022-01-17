/*
 * Copyright (C) Ascensio System SIA 2012-2019. All rights reserved
 *
 * https://www.onlyoffice.com/ 
 *
 * Version: 5.3.4 (build:3)
 */


'use strict';

var pg = require('pg');
var co = require('co');
var pgEscape = require('pg-escape');
var types = require('pg').types;
var sqlBase = require('./baseConnector');
var configSql = require('config').get('services.CoAuthoring.sql');
var pool = new pg.Pool({
  host: configSql.get('dbHost'),
  port: configSql.get('dbPort'),
  user: configSql.get('dbUser'),
  password: configSql.get('dbPass'),
  database: configSql.get('dbName'),
  max: configSql.get('connectionlimit'),
  min: 0,
  ssl: false,
  idleTimeoutMillis: 30000
});
types.setTypeParser(1114, function(stringValue) {
  return new Date(stringValue + '+0000');
});
types.setTypeParser(1184, function(stringValue) {
  return new Date(stringValue + '+0000');
});

var logger = require('./../../Common/sources/logger');

exports.sqlQuery = function(sqlCommand, callbackFunction, opt_noModifyRes, opt_noLog) {
  co(function *() {
    var client = null;
    var result = null;
    var error = null;
    try {
      client = yield pool.connect();
      result = yield client.query(sqlCommand);
    } catch (err) {
      error = err;
      if (!opt_noLog) {
        if (client) {
          logger.error('sqlQuery error sqlCommand: %s:\r\n%s', sqlCommand.slice(0, 50), err.stack);
        } else {
          logger.error('pool.getConnection error: %s', err);
        }
      }
    } finally {
      if (client) {
        client.release();
      }
      if (callbackFunction) {
        var output = result;
        if (result && !opt_noModifyRes) {
          if ('SELECT' === result.command) {
            output = result.rows;
          } else {
            output = {affectedRows: result.rowCount};
          }
        }
        callbackFunction(error, output);
      }
    }
  });
};
exports.sqlEscape = function(value) {
  return undefined !== value ? pgEscape.literal(value.toString()) : 'NULL';
};
var isSupportOnConflict = false;
(function checkIsSupportOnConflict() {
  var sqlCommand = 'INSERT INTO checkIsSupportOnConflict (id) VALUES(1) ON CONFLICT DO NOTHING;';
  exports.sqlQuery(sqlCommand, function(error, result) {
    if (error) {
      if ('42601' == error.code) {
        isSupportOnConflict = false;
        logger.debug('checkIsSupportOnConflict false');
      } else if ('42P01' == error.code) {
        isSupportOnConflict = true;
        logger.debug('checkIsSupportOnConflict true');
      } else {
        logger.error('checkIsSupportOnConflict unexpected error code:\r\n%s', error.stack);
      }
    }
  }, true, true);
})();

function getUpsertString(task) {
  task.completeDefaults();
  var dateNow = sqlBase.getDateTime(new Date());
  var commandArg = [task.key, task.status, task.statusInfo, dateNow, task.userIndex, task.changeId, task.callback, task.baseurl];
  var commandArgEsc = commandArg.map(function(curVal) {
    return exports.sqlEscape(curVal)
  });
  if (isSupportOnConflict) {
    return "INSERT INTO task_result (id, status, status_info, last_open_date, user_index, change_id, callback," +
      " baseurl) SELECT " + commandArgEsc.join(', ') + " WHERE 'false' = set_config('myapp.isupdate', 'false', true) " +
      "ON CONFLICT (id) DO UPDATE SET  last_open_date = " +
      sqlBase.baseConnector.sqlEscape(dateNow) +
      ", user_index = task_result.user_index + 1 WHERE 'true' = set_config('myapp.isupdate', 'true', true) RETURNING" +
      " current_setting('myapp.isupdate') as isupdate, user_index as userindex;";
  } else {
    return "SELECT * FROM merge_db(" + commandArgEsc.join(', ') + ");";
  }
}
exports.upsert = function(task) {
  return new Promise(function(resolve, reject) {
    var sqlCommand = getUpsertString(task);
    exports.sqlQuery(sqlCommand, function(error, result) {
      if (error) {
        reject(error);
      } else {
        if (result && result.rows.length > 0) {
          var first = result.rows[0];
          result = {affectedRows: 0, insertId: 0};
          result.affectedRows = 'true' == first.isupdate ? 2 : 1;
          result.insertId = first.userindex;
        }
        resolve(result);
      }
    }, true);
  });
};
