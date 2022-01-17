/*
 * Copyright (C) Ascensio System SIA 2012-2019. All rights reserved
 *
 * https://www.onlyoffice.com/ 
 *
 * Version: 5.3.4 (build:3)
 */


'use strict';

const sockjs = require('sockjs');
const nodehun = require('nodehun');
const logger = require('./../../Common/sources/logger');
const utils = require('./../../Common/sources/utils');
const fs = require('fs');
const co = require('co');
const cfgSockjs = require('config').get('services.CoAuthoring.sockjs');
const languages = require('./languages');
const allLanguages = languages.allLanguages;
const path = require('path');
const arrExistDictionaries = {};
const pathDictionaries = path.join(__dirname, '../dictionaries');
const arrDictionaries = {};

function spell(type, word, id) {
	return new Promise(function(resolve, reject) {
		let dict = null;
		if (arrDictionaries[id]) {
			dict = arrDictionaries[id];
		} else {
			if (arrExistDictionaries[id]) {
				let pathTmp = path.join(pathDictionaries, allLanguages[id], allLanguages[id] + '.');
				dict = arrDictionaries[id] = new nodehun(pathTmp + 'aff', pathTmp + 'dic');
			}
		}

		if (dict) {
			if ("spell" === type) {
				dict.isCorrect(word, function (err, correct, origWord) {
					return setImmediate(resolve, !err && correct);
				});
			} else if ("suggest" === type) {
				dict.spellSuggestions(word, function (err, correct, suggestions, origWord) {
					return setImmediate(resolve, suggestions);
				});
			}
		} else {
			return setImmediate(resolve, true);
		}
	});
}
 
exports.install = function (server, callbackFunction) {
	'use strict';

	utils.listFolders(pathDictionaries, true).then((values) => {
		return co(function*() {
			let lang;
			for (let i = 0; i < values.length; ++i) {
				lang = languages.sToId(path.basename(values[i]));
				if (-1 !== lang) {
					arrExistDictionaries[lang] = 1;
				}
			}
			yield spell('spell', 'color', 0x0409);
			callbackFunction();
		});
	});

	const sockjs_echo = sockjs.createServer(cfgSockjs);

	sockjs_echo.on('connection', function (conn) {
		if (!conn) {
			logger.error ("null == conn");
			return;
		}
		conn.on('data', function (message) {
			try {
				let data = JSON.parse(message);
				switch (data.type) {
					case 'spellCheck':	spellCheck(conn, data.spellCheckData);break;
				}
			} catch (e) {
				logger.error("error receiving response: %s", e);
			}
		});
		conn.on('error', function () {
			logger.error("On error");
		});
		conn.on('close', function () {
			logger.info("Connection closed or timed out");
		});

		sendData(conn, {type: 'init', languages: Object.keys(arrExistDictionaries)});
	});

	function sendData(conn, data) {
		conn.write(JSON.stringify(data));
	}

	function spellCheck(conn, data) {
		return co(function*() {
			let promises = [];
			for (let i = 0, length = data.usrWords.length; i < length; ++i) {
				promises.push(spell(data.type, data.usrWords[i], data.usrLang[i]));
			}
			yield Promise.all(promises).then(values => {
				data[('spell' === data.type ? 'usrCorrect' : 'usrSuggest')] = values;
			});
			sendData(conn, {type: 'spellCheck', spellCheckData: data});
		});
	}

	sockjs_echo.installHandlers(server, {prefix:'/doc/[0-9-.a-zA-Z_=]*/c', log:function (severity, message) {
		logger.info(message);
	}});
};
exports.spellSuggest = function (type, word, lang, callbackFunction) {
	return co(function*() {
		callbackFunction(yield spell(type, word, lang));
	});
};
