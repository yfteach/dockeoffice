/*
 * Copyright (C) Ascensio System SIA 2012-2019. All rights reserved
 *
 * https://www.onlyoffice.com/ 
 *
 * Version: 5.3.4 (build:3)
 */


'use strict';

var mongoDB = require('mongodb');
var config = require('./config.json');
var _errorConnection = true;

var logger = require('./../../Common/sources/logger');

function CreateDbClient(){
	return new mongoDB.Db(config['mongodb']['database'], new mongoDB.Server(config['mongodb']['host'], config['mongodb']['port'], {auto_reconnect: true}), {safe:false});
}
exports.insert = function (_collectionName, _newElement) {
	var _db = CreateDbClient();
	if (!_db) {
		logger.error ("Error _db");
		return;
	}
	_db.open (function (err, db) {
		if (!err) {
			db.collection(_collectionName, function(err, collection) {
				if (!err) {
					collection.insert (_newElement);
				} else {
					logger.error ("Error collection");
					return;
				}
				
				db.close();
			});
		} else {
			logger.error ("Error open database");
		}
	});
};
exports.remove = function (_collectionName, _removeElements) {
	var _db = CreateDbClient();
	if (!_db) {
		logger.error ("Error _db");
		return;
	}
	_db.open (function (err, db) {
		if (!err) {
			db.collection(_collectionName, function(err, collection) {
				if (!err) {
					collection.remove (_removeElements, function(err, collection) { 
						logger.info ("All elements remove");
					});
				} else {
					logger.error ("Error collection");
					return;
				}
				
				db.close();
			});
		} else {
			logger.error ("Error open database");
		}
	});
};
exports.load = function (_collectionName, callbackFunction) {
	var _db = CreateDbClient();
	if (!_db) {
		logger.error ("Error _db");
		return callbackFunction (null);
	}
	
	var result = [];
	_db.open (function (err, db) {
		db.collection(_collectionName, function(err, collection) {
			collection.find(function(err, cursor) {
				cursor.each(function(err, item) {
					if (item != null) {
						if (!result.hasOwnProperty (item.docid))
							result[item.docid] = [item];
						else
							result[item.docid].push(item);
					} else
						callbackFunction (result);
				});
				
				db.close();
			});
		});
	});
};