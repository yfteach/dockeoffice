/*
 * Copyright (C) Ascensio System SIA 2012-2019. All rights reserved
 *
 * https://www.onlyoffice.com/ 
 *
 * Version: 5.3.4 (build:3)
 */


'use strict';

const cluster = require('cluster');
const logger = require('./../../Common/sources/logger');

if (cluster.isMaster) {
  const fs = require('fs');
  const co = require('co');
  const numCPUs = require('os').cpus().length;
  const configCommon = require('config');
  const config = configCommon.get('FileConverter.converter');
  const license = require('./../../Common/sources/license');

  const cfgMaxProcessCount = config.get('maxprocesscount');
  var licenseInfo, workersCount = 0;
  const readLicense = function* () {
    licenseInfo = yield* license.readLicense();
    workersCount = Math.min(licenseInfo.count, Math.ceil(numCPUs * cfgMaxProcessCount));
  };
  const updateWorkers = () => {
    var i;
    const arrKeyWorkers = Object.keys(cluster.workers);
    if (arrKeyWorkers.length < workersCount) {
      for (i = arrKeyWorkers.length; i < workersCount; ++i) {
        const newWorker = cluster.fork();
        logger.warn('worker %s started.', newWorker.process.pid);
      }
    } else {
      for (i = workersCount; i < arrKeyWorkers.length; ++i) {
        const killWorker = cluster.workers[arrKeyWorkers[i]];
        if (killWorker) {
          killWorker.kill();
        }
      }
    }
  };
  const updateLicense = () => {
    return co(function*() {
      try {
        yield* readLicense();
        logger.warn('update cluster with %s workers', workersCount);
        updateWorkers();
      } catch (err) {
        logger.error('updateLicense error:\r\n%s', err.stack);
      }
    });
  };

  cluster.on('exit', (worker, code, signal) => {
    logger.warn('worker %s died (code = %s; signal = %s).', worker.process.pid, code, signal);
    updateWorkers();
  });

  updateLicense();

  fs.watchFile(configCommon.get('license').get('license_file'), updateLicense);
  setInterval(updateLicense, 86400000);
} else {
  const converter = require('./converter');
  converter.run();
}

process.on('uncaughtException', (err) => {
  logger.error((new Date).toUTCString() + ' uncaughtException:', err.message);
  logger.error(err.stack);
  logger.shutdown(() => {
    process.exit(1);
  });
});
