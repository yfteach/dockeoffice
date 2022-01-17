/*
 * Copyright (C) Ascensio System SIA 2012-2019. All rights reserved
 *
 * https://www.onlyoffice.com/ 
 *
 * Version: 5.3.4 (build:3)
 */


'use strict';

var sqlDataBaseType = {
	mySql		: 'mysql',
	postgreSql	: 'postgres'
};

var config = require('config').get('services.CoAuthoring.sql');
var baseConnector = (sqlDataBaseType.mySql === config.get('type')) ? require('./mySqlBaseConnector') : require('./postgreSqlBaseConnector');

var tableChanges = config.get('tableChanges'),
	tableResult = config.get('tableResult');

var g_oCriticalSection = {};
var maxPacketSize = config.get('max_allowed_packet'); // Размер по умолчанию для запроса в базу данных 1Mb - 1 (т.к. он не пишет 1048575, а пишет 1048574)

function getDataFromTable (tableId, data, getCondition, callback) {
	var table = getTableById(tableId);
	var sqlCommand = "SELECT " + data + " FROM " + table + " WHERE " + getCondition + ";";

	baseConnector.sqlQuery(sqlCommand, callback);
}
function deleteFromTable (tableId, deleteCondition, callback) {
	var table = getTableById(tableId);
	var sqlCommand = "DELETE FROM " + table + " WHERE " + deleteCondition + ";";

	baseConnector.sqlQuery(sqlCommand, callback);
}
var c_oTableId = {
	callbacks	: 2,
	changes		: 3
};
function getTableById (id) {
	var res;
	switch (id) {
		case c_oTableId.changes:
			res = tableChanges;
			break;
	}
	return res;
}

exports.baseConnector = baseConnector;
exports.tableId = c_oTableId;
exports.loadTable = function (tableId, callbackFunction) {
	var table = getTableById(tableId);
	var sqlCommand = "SELECT * FROM " + table + ";";
	baseConnector.sqlQuery(sqlCommand, callbackFunction);
};
exports.insertChanges = function (objChanges, docId, index, user) {
	lockCriticalSection(docId, function () {_insertChanges(0, objChanges, docId, index, user);});
};
exports.insertChangesPromise = function (objChanges, docId, index, user) {
  return new Promise(function(resolve, reject) {
    _insertChangesCallback(0, objChanges, docId, index, user, function(error, result) {
      if (error) {
        reject(error);
      } else {
        resolve(result);
      }
    });
  });
};
function _lengthInUtf8Bytes (s) {
	return ~-encodeURI(s).split(/%..|./).length;
}
function _getDateTime2(oDate) {
  return oDate.toISOString().slice(0, 19).replace('T', ' ');
}
function _getDateTime(nTime) {
	var oDate = new Date(nTime);
  return _getDateTime2(oDate);
}

exports.getDateTime = _getDateTime2;
function _insertChanges (startIndex, objChanges, docId, index, user) {
  _insertChangesCallback(startIndex, objChanges, docId, index, user, function () {unLockCriticalSection(docId);});
}
function _insertChangesCallback (startIndex, objChanges, docId, index, user, callback) {
	var sqlCommand = "INSERT INTO " + tableChanges + " VALUES";
	var i = startIndex, l = objChanges.length, sqlNextRow = "", lengthUtf8Current = 0, lengthUtf8Row = 0;
	if (i === l)
		return;

	for (; i < l; ++i, ++index) {
		sqlNextRow = "(" + baseConnector.sqlEscape(docId) + "," + baseConnector.sqlEscape(index) + ","
			+ baseConnector.sqlEscape(user.id) + "," + baseConnector.sqlEscape(user.idOriginal) + ","
			+ baseConnector.sqlEscape(user.username) + "," + baseConnector.sqlEscape(objChanges[i].change) + ","
			+ baseConnector.sqlEscape(_getDateTime(objChanges[i].time)) + ")";
		lengthUtf8Row = _lengthInUtf8Bytes(sqlNextRow) + 1; // 1 - это на символ ',' или ';' в конце команды
		if (i === startIndex) {
			lengthUtf8Current = _lengthInUtf8Bytes(sqlCommand);
			sqlCommand += sqlNextRow;
		} else {
			if (lengthUtf8Row + lengthUtf8Current >= maxPacketSize) {
				sqlCommand += ';';
				(function (tmpStart, tmpIndex) {
					baseConnector.sqlQuery(sqlCommand, function () {
						_insertChangesCallback(tmpStart, objChanges, docId, tmpIndex, user, callback);
					});
				})(i, index);
				return;
			} else {
				sqlCommand += ',';
				sqlCommand += sqlNextRow;
			}
		}

		lengthUtf8Current += lengthUtf8Row;
	}

	sqlCommand += ';';
	baseConnector.sqlQuery(sqlCommand, callback);
}
exports.deleteChangesCallback = function (docId, deleteIndex, callback) {
  var sqlCommand = "DELETE FROM " + tableChanges + " WHERE id='" + docId + "'";
  if (null !== deleteIndex)
    sqlCommand += " AND change_id >= " + deleteIndex;
  sqlCommand += ";";
  baseConnector.sqlQuery(sqlCommand, callback);
};
exports.deleteChangesPromise = function (docId, deleteIndex) {
  return new Promise(function(resolve, reject) {
    exports.deleteChangesCallback(docId, deleteIndex, function(error, result) {
      if (error) {
        reject(error);
      } else {
        resolve(result);
      }
    });
  });
};
exports.deleteChanges = function (docId, deleteIndex) {
	lockCriticalSection(docId, function () {_deleteChanges(docId, deleteIndex);});
};
function _deleteChanges (docId, deleteIndex) {
  exports.deleteChangesCallback(docId, deleteIndex, function () {unLockCriticalSection(docId);});
}
exports.getChangesIndex = function(docId, callback) {
  var table = getTableById(c_oTableId.changes);
  var sqlCommand = 'SELECT MAX(change_id) as change_id FROM ' + table + ' WHERE id=' + baseConnector.sqlEscape(docId) + ';';
  baseConnector.sqlQuery(sqlCommand, callback);
};
exports.getChangesIndexPromise = function(docId) {
  return new Promise(function(resolve, reject) {
    exports.getChangesIndex(docId, function(error, result) {
      if (error) {
        reject(error);
      } else {
        resolve(result);
      }
    });
  });
};
exports.getChangesPromise = function (docId, optStartIndex, optEndIndex, opt_time) {
  return new Promise(function(resolve, reject) {
    var getCondition = 'id='+baseConnector.sqlEscape(docId);
    if (null != optStartIndex) {
      getCondition += ' AND change_id>=' + optStartIndex;
    }
    if (null != optEndIndex) {
      getCondition += ' AND change_id<' + optEndIndex;
    }
    if (null != opt_time) {
      getCondition += ' AND change_date<=' + baseConnector.sqlEscape(_getDateTime(opt_time));
    }
    getCondition += ' ORDER BY change_id ASC';
    getDataFromTable(c_oTableId.changes, "*", getCondition, function(error, result) {
      if (error) {
        reject(error);
      } else {
        resolve(result);
      }
    });
  });
};
exports.checkStatusFile = function (docId, callbackFunction) {
	var sqlCommand = "SELECT status, status_info FROM " + tableResult + " WHERE id='" + docId + "';";
	baseConnector.sqlQuery(sqlCommand, callbackFunction);
};
exports.checkStatusFilePromise = function (docId) {
  return new Promise(function(resolve, reject) {
    exports.checkStatusFile(docId, function(error, result) {
      if (error) {
        reject(error);
      } else {
        resolve(result);
      }
    });
  });
};
exports.updateStatusFile = function (docId) {
	var sqlCommand = "UPDATE " + tableResult + " SET status=1 WHERE id='" + docId + "';";
	baseConnector.sqlQuery(sqlCommand);
};

exports.isLockCriticalSection = function (id) {
	return !!(g_oCriticalSection[id]);
};
function lockCriticalSection (id, callback) {
	if (g_oCriticalSection[id]) {
		g_oCriticalSection[id].push(callback);
		return;
	}
	g_oCriticalSection[id] = [];
	g_oCriticalSection[id].push(callback);
	callback();
}
function unLockCriticalSection (id) {
	var arrCallbacks = g_oCriticalSection[id];
	arrCallbacks.shift();
	if (0 < arrCallbacks.length)
		arrCallbacks[0]();
	else
		delete g_oCriticalSection[id];
}
exports.healthCheck = function () {
  return new Promise(function(resolve, reject) {
    baseConnector.sqlQuery('SELECT 1;', function(error, result) {
      if (error) {
        reject(error);
      } else {
        resolve(result);
      }
    });
  });
};

exports.getEmptyCallbacks = function() {
  return new Promise(function(resolve, reject) {
    const sqlCommand = "SELECT DISTINCT t1.id FROM doc_changes t1 LEFT JOIN task_result t2 ON t2.id = t1.id WHERE t2.callback = '';";
    baseConnector.sqlQuery(sqlCommand, function(error, result) {
      if (error) {
        reject(error);
      } else {
        resolve(result);
      }
    });
  });
};
