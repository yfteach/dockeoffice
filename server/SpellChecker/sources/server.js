/*
 * Copyright (C) Ascensio System SIA 2012-2019. All rights reserved
 *
 * https://www.onlyoffice.com/ 
 *
 * Version: 5.3.4 (build:3)
 */

'use strict';

const cluster = require('cluster');
const config = require('config').get('SpellChecker');

const logger = require('./../../Common/sources/logger');

const c_nCheckHealth = 60000, c_sCheckWord = 'color', c_sCheckLang = 1033;
let idCheckInterval, canStartCheck = true;
let statusCheckHealth = true;
function checkHealth (worker) {
	logger.info('checkHealth');
	if (!statusCheckHealth) {
		logger.error('error check health, restart!');
		worker.kill();
		return;
	}
	worker.send({type: 'spell'});
	statusCheckHealth = false;
}
function endCheckHealth (msg) {
	logger.info('endCheckHealth');
	statusCheckHealth = true;
}

const workersCount = 1;	// ToDo Пока только 1 процесс будем задействовать. Но в будующем стоит рассмотреть несколько.
if (cluster.isMaster) {
	logger.warn('start cluster with %s workers', workersCount);
	cluster.on('listening', function(worker) {
		if (canStartCheck) {
			canStartCheck = false;
			idCheckInterval = setInterval(function(){checkHealth(worker);}, c_nCheckHealth);
			worker.on('message', function(msg){endCheckHealth(msg);});
		}
	});
	for (let nIndexWorker = 0; nIndexWorker < workersCount; ++nIndexWorker) {
		logger.warn('worker %s started.', cluster.fork().process.pid);
	}

	cluster.on('exit', (worker, code, signal) => {
		logger.warn('worker %s died (code = %s; signal = %s). restart...', worker.process.pid, code, signal);
		clearInterval(idCheckInterval);
		endCheckHealth();
		canStartCheck = true;
		cluster.fork();
	});
} else {
	const express = require('express'),
		http = require('http'),
		https = require('https'),
		fs = require("fs"),
		app = express(),
		spellCheck  = require('./spellCheck');
	let server = null;


	logger.warn('Express server starting...');

	if (config.has('ssl')) {
		const privateKey = fs.readFileSync(config.get('ssl.key')).toString();
		const certificateKey = fs.readFileSync(config.get('ssl.cert')).toString();
		const trustedCertificate = fs.readFileSync(config.get('ssl.ca')).toString();
		const options = {key: privateKey, cert: certificateKey, ca: [trustedCertificate]};

		server = https.createServer(options, app);
	} else {
		server = http.createServer(app);
	}
	spellCheck.install(server, function(){
		server.listen(config.get('server.port'), function(){
			logger.warn("Express server listening on port %d in %s mode", config.get('server.port'), app.settings.env);
		});

		app.get('/index.html', function(req, res) {
			res.send('Server is functioning normally');
		});
	});

	process.on('message', function(msg) {
		if (!spellCheck)
			return;
		spellCheck.spellSuggest(msg.type, c_sCheckWord, c_sCheckLang, function(res) {
			process.send({type: msg.type, res: res});
		});
	});

	process.on('uncaughtException', function(err) {
		logger.error((new Date).toUTCString() + ' uncaughtException:', err.message);
		logger.error(err.stack);
		logger.shutdown(function () {
			process.exit(1);
		});
	});
}
