/*
 * Copyright (C) Ascensio System SIA 2012-2019. All rights reserved
 *
 * https://www.onlyoffice.com/ 
 *
 * Version: 5.3.4 (build:3)
 */

'use strict';

const cluster = require('cluster');
const configCommon = require('config');
const config = configCommon.get('services.CoAuthoring');
const logger = require('./../../Common/sources/logger');
const co = require('co');
const license = require('./../../Common/sources/license');

if (cluster.isMaster) {
	const fs = require('fs');
	let licenseInfo, workersCount = 0, updateTime;
	const readLicense = function*() {
		licenseInfo = yield* license.readLicense();
		workersCount = Math.min(1, licenseInfo.count/*, Math.ceil(numCPUs * cfgWorkerPerCpu)*/);
	};
	const updateLicenseWorker = (worker) => {
		worker.send({type: 1, data: licenseInfo});
	};
	const updateWorkers = () => {
		const arrKeyWorkers = Object.keys(cluster.workers);
		if (arrKeyWorkers.length < workersCount) {
			for (let i = arrKeyWorkers.length; i < workersCount; ++i) {
				const newWorker = cluster.fork();
				logger.warn('worker %s started.', newWorker.process.pid);
			}
		} else {
			for (let i = workersCount; i < arrKeyWorkers.length; ++i) {
				const killWorker = cluster.workers[arrKeyWorkers[i]];
				if (killWorker) {
					killWorker.kill();
				}
			}
		}
	};
	const updatePlugins = (eventType, filename) => {
		console.log('update Folder: %s ; %s', eventType, filename);
		if (updateTime && 1000 >= (new Date() - updateTime)) {
			return;
		}
		console.log('update Folder true: %s ; %s', eventType, filename);
		updateTime = new Date();
		for (let i in cluster.workers) {
			cluster.workers[i].send({type: 2});
		}
	};
	const updateLicense = () => {
		return co(function*() {
			try {
				yield* readLicense();
				logger.warn('update cluster with %s workers', workersCount);
				for (let i in cluster.workers) {
					updateLicenseWorker(cluster.workers[i]);
				}
				updateWorkers();
			} catch (err) {
				logger.error('updateLicense error:\r\n%s', err.stack);
			}
		});
	};

	cluster.on('fork', (worker) => {
		updateLicenseWorker(worker);
	});
	cluster.on('exit', (worker, code, signal) => {
		logger.warn('worker %s died (code = %s; signal = %s).', worker.process.pid, code, signal);
		updateWorkers();
	});

	updateLicense();

	try {
		fs.watch(config.get('plugins.path'), updatePlugins);
	} catch (e) {
		logger.warn('Plugins watch exception (https://nodejs.org/docs/latest/api/fs.html#fs_availability).');
	}
	fs.watchFile(configCommon.get('license').get('license_file'), updateLicense);
	setInterval(updateLicense, 86400000);
} else {
	logger.warn('Express server starting...');

	const express = require('express');
	const http = require('http');
	const urlModule = require('url');
	const path = require('path');
	const bodyParser = require("body-parser");
	const mime = require('mime');
	const docsCoServer = require('./DocsCoServer');
	const canvasService = require('./canvasservice');
	const converterService = require('./converterservice');
	const fileUploaderService = require('./fileuploaderservice');
	const constants = require('./../../Common/sources/constants');
	const utils = require('./../../Common/sources/utils');
	const commonDefines = require('./../../Common/sources/commondefines');
	const configStorage = configCommon.get('storage');
	const app = express();
	const server = http.createServer(app);

	let userPlugins = null, updatePlugins = true;

	if (config.has('server.static_content')) {
		const staticContent = config.get('server.static_content');
		for (let i in staticContent) {
			app.use(i, express.static(staticContent[i]['path'], staticContent[i]['options']));
		}
	}

	if (configStorage.has('fs.folderPath')) {
		const cfgBucketName = configStorage.get('bucketName');
		const cfgStorageFolderName = configStorage.get('storageFolderName');
		app.use('/' + cfgBucketName + '/' + cfgStorageFolderName, (req, res, next) => {
			const index = req.url.lastIndexOf('/');
			if ('GET' === req.method && -1 != index) {
				const contentDisposition = req.query['disposition'] || 'attachment';
				let sendFileOptions = {
					root: configStorage.get('fs.folderPath'), dotfiles: 'deny', headers: {
						'Content-Disposition': contentDisposition
					}
				};
				const urlParsed = urlModule.parse(req.url);
				if (urlParsed && urlParsed.pathname) {
					const filename = decodeURIComponent(path.basename(urlParsed.pathname));
					sendFileOptions.headers['Content-Type'] = mime.getType(filename);
				}
				const realUrl = req.url.substring(0, index);
				res.sendFile(realUrl, sendFileOptions, (err) => {
					if (err) {
						logger.error(err);
						res.status(400).end();
					}
				});
			} else {
				res.sendStatus(404)
			}
		});
	}
	docsCoServer.install(server, () => {
		server.listen(config.get('server.port'), () => {
			logger.warn("Express server listening on port %d in %s mode", config.get('server.port'), app.settings.env);
		});

		app.get('/index.html', (req, res) => {
			res.send('Server is functioning normally. Version: ' + commonDefines.buildVersion + '. Build: ' +
				commonDefines.buildNumber);
		});
		const rawFileParser = bodyParser.raw(
			{inflate: true, limit: config.get('server.limits_tempfile_upload'), type: '*/*'});

		app.get('/coauthoring/CommandService.ashx', utils.checkClientIp, rawFileParser, docsCoServer.commandFromServer);
		app.post('/coauthoring/CommandService.ashx', utils.checkClientIp, rawFileParser,
			docsCoServer.commandFromServer);

		app.get('/ConvertService.ashx', utils.checkClientIp, rawFileParser, converterService.convertXml);
		app.post('/ConvertService.ashx', utils.checkClientIp, rawFileParser, converterService.convertXml);
		app.post('/converter', utils.checkClientIp, rawFileParser, converterService.convertJson);


		app.get('/FileUploader.ashx', utils.checkClientIp, rawFileParser, fileUploaderService.uploadTempFile);
		app.post('/FileUploader.ashx', utils.checkClientIp, rawFileParser, fileUploaderService.uploadTempFile);

		const docIdRegExp = new RegExp("^[" + constants.DOC_ID_PATTERN + "]*$", 'i');
		app.param('docid', (req, res, next, val) => {
			if (docIdRegExp.test(val)) {
				next();
			} else {
				res.sendStatus(403);
			}
		});
		app.param('index', (req, res, next, val) => {
			if (!isNaN(parseInt(val))) {
				next();
			} else {
				res.sendStatus(403);
			}
		});
		app.post('/uploadold/:docid/:userid/:index', fileUploaderService.uploadImageFileOld);
		app.post('/upload/:docid/:userid/:index', rawFileParser, fileUploaderService.uploadImageFile);

		app.post('/downloadas/:docid', rawFileParser, canvasService.downloadAs);
		app.post('/savefile/:docid', rawFileParser, canvasService.saveFile);
		app.get('/healthcheck', utils.checkClientIp, docsCoServer.healthCheck);

		app.get('/baseurl', (req, res) => {
			res.send(utils.getBaseUrlByRequest(req));
		});
		
		app.get('/robots.txt', (req, res) => {
			res.setHeader('Content-Type', 'plain/text');
			res.send("User-agent: *\nDisallow: /");
		});

		app.post('/docbuilder', utils.checkClientIp, rawFileParser, (req, res) => {
			converterService.builder(req, res);
		});
		app.get('/info/info.json', utils.checkClientIp, docsCoServer.licenseInfo);

		const sendUserPlugins = (res, data) => {
			res.setHeader('Content-Type', 'application/json');
			res.send(JSON.stringify(data));
		};
		app.get('/plugins.json', (req, res) => {
			if (userPlugins && !updatePlugins) {
				sendUserPlugins(res, userPlugins);
				return;
			}

			if (!config.has('server.static_content') || !config.has('plugins.uri')) {
				res.sendStatus(404);
				return;
			}

			let staticContent = config.get('server.static_content');
			let pluginsUri = config.get('plugins.uri');
			let pluginsPath = undefined;
			let pluginsAutostart = config.get('plugins.autostart');

			if (staticContent[pluginsUri]) {
				pluginsPath = staticContent[pluginsUri].path;
			}

			let baseUrl = '../../../..';
			utils.listFolders(pluginsPath, true).then((values) => {
				return co(function*() {
					const configFile = 'config.json';
					let stats = null;
					let result = [];
					for (let i = 0; i < values.length; ++i) {
						try {
							stats = yield utils.fsStat(path.join(values[i], configFile));
						} catch (err) {
							stats = null;
						}

						if (stats && stats.isFile) {
							result.push( baseUrl + pluginsUri + '/' + path.basename(values[i]) + '/' + configFile);
						}
					}

					// userPlugins = {'url': '', 'pluginsData': result, 'autostart': pluginsAutostart};
					userPlugins = {'url': '', 'pluginsData': [], 'autostart': pluginsAutostart};
					sendUserPlugins(res, userPlugins);
				});
			});
		});
	});

	process.on('message', (msg) => {
		if (!docsCoServer) {
			return;
		}
		switch (msg.type) {
			case 1:
				docsCoServer.setLicenseInfo(msg.data);
				break;
			case 2:
				updatePlugins = true;
				break;
		}
	});
}

process.on('uncaughtException', (err) => {
	logger.error((new Date).toUTCString() + ' uncaughtException:', err.message);
	logger.error(err.stack);
	logger.shutdown(() => {
		process.exit(1);
	});
});
