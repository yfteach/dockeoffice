/*
 * Copyright (C) Ascensio System SIA 2012-2019. All rights reserved
 *
 * https://www.onlyoffice.com/ 
 *
 * Version: 5.3.4 (build:3)
 */


'use strict';

var mysql = require('mysql');
var sqlBase = require('./baseConnector');
var configSql = require('config').get('services.CoAuthoring.sql');
var pool  = mysql.createPool({
	host		: configSql.get('dbHost'),
	port		: configSql.get('dbPort'),
	user		: configSql.get('dbUser'),
	password	: configSql.get('dbPass'),
	database	: configSql.get('dbName'),
	charset		: configSql.get('charset'),
	connectionLimit	: configSql.get('connectionlimit'),
	timezone	: '+0000',
	flags : '-FOUND_ROWS'
});
var logger = require('./../../Common/sources/logger');

exports.sqlQuery = function (sqlCommand, callbackFunction) {
	pool.getConnection(function(err, connection) {
		if (err) {
			logger.error('pool.getConnection error: %s', err);
			if (callbackFunction) callbackFunction(err, null);
			return;
		}

		connection.query(sqlCommand, function (error, result) {
			connection.release();
			if (error) {
				logger.error('________________________error_____________________');
				logger.error('sqlQuery: %s sqlCommand: %s', error.code, sqlCommand);
				logger.error(error);
				logger.error('_____________________end_error_____________________');
			}
			if (callbackFunction) callbackFunction(error, result);
		});
	});
};
exports.sqlEscape = function (value) {
	return pool.escape(value);
};

function getUpsertString(task, opt_updateUserIndex) {
	task.completeDefaults();
	var dateNow = sqlBase.getDateTime(new Date());
	var commandArg = [task.key, task.status, task.statusInfo, dateNow, task.userIndex, task.changeId, task.callback, task.baseurl];
	var commandArgEsc = commandArg.map(function(curVal) {
		return exports.sqlEscape(curVal)
	});
	var sql = 'INSERT INTO task_result ( id, status, status_info, last_open_date,' +
		' user_index, change_id, callback, baseurl ) VALUES (' + commandArgEsc.join(', ') + ') ON DUPLICATE KEY UPDATE' +
		' last_open_date = ' + exports.sqlEscape(dateNow);
	if (opt_updateUserIndex) {
		sql += ', user_index = LAST_INSERT_ID(user_index + 1);';
	} else {
		sql += ';';
	}
	return sql;
}

exports.upsert = function(task, opt_updateUserIndex) {
	return new Promise(function(resolve, reject) {
		var sqlCommand = getUpsertString(task, opt_updateUserIndex);
		exports.sqlQuery(sqlCommand, function(error, result) {
			if (error) {
				reject(error);
			} else {
				resolve(result);
			}
		});
	});
};
