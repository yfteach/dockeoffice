/*
 * Copyright (C) Ascensio System SIA 2012-2019. All rights reserved
 *
 * https://www.onlyoffice.com/ 
 *
 * Version: 5.3.4 (build:3)
 */


'use strict';

var config = require('config');

var log4js = require('log4js');
log4js.configure(config.get('log.filePath'));

var logger = log4js.getLogger('nodeJS');

if (config.get('log.options.replaceConsole')) {
	console.log = logger.info.bind(logger);
	console.info = logger.info.bind(logger);
	console.warn = logger.warn.bind(logger);
	console.error = logger.error.bind(logger);
	console.debug = logger.debug.bind(logger);
}

exports.trace = function (){
	return logger.trace.apply(logger, Array.prototype.slice.call(arguments));
};
exports.debug = function (){
	return logger.debug.apply(logger, Array.prototype.slice.call(arguments));
};
exports.info = function (){
	return logger.info.apply(logger, Array.prototype.slice.call(arguments));
};
exports.warn = function (){
	return logger.warn.apply(logger, Array.prototype.slice.call(arguments));
};
exports.error = function (){
	return logger.error.apply(logger, Array.prototype.slice.call(arguments));
};
exports.fatal = function (){
	return logger.fatal.apply(logger, Array.prototype.slice.call(arguments));
};
exports.shutdown = function (callback) {
	return log4js.shutdown(callback);
};