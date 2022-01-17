/*
 * Copyright (C) Ascensio System SIA 2012-2019. All rights reserved
 *
 * https://www.onlyoffice.com/ 
 *
 * Version: 5.3.4 (build:3)
 */


'use strict';

var config = require('config').get('services.CoAuthoring');
var co = require('co');
var cron = require('cron');
var ms = require('ms');
var taskResult = require('./taskresult');
var docsCoServer = require('./DocsCoServer');
var canvasService = require('./canvasservice');
var storage = require('./../../Common/sources/storage-base');
var utils = require('./../../Common/sources/utils');
var logger = require('./../../Common/sources/logger');
var constants = require('./../../Common/sources/constants');
var commondefines = require('./../../Common/sources/commondefines');
var pubsubRedis = require('./pubsubRedis.js');
var queueService = require('./../../Common/sources/taskqueueRabbitMQ');
var pubsubService = require('./' + config.get('pubsub.name'));

var cfgRedisPrefix = config.get('redis.prefix');
var cfgExpFilesCron = config.get('expire.filesCron');
var cfgExpDocumentsCron = config.get('expire.documentsCron');
var cfgExpFiles = config.get('expire.files');
var cfgExpFilesRemovedAtOnce = config.get('expire.filesremovedatonce');
var cfgForceSaveEnable = config.get('autoAssembly.enable');
var cfgForceSaveStep = ms(config.get('autoAssembly.step'));

var redisKeyDocuments = cfgRedisPrefix + constants.REDIS_KEY_DOCUMENTS;
var redisKeyForceSaveTimer = cfgRedisPrefix + constants.REDIS_KEY_FORCE_SAVE_TIMER;
var redisKeyForceSaveTimerLock = cfgRedisPrefix + constants.REDIS_KEY_FORCE_SAVE_TIMER_LOCK;

var checkFileExpire = function() {
  return co(function* () {
    try {
      logger.debug('checkFileExpire start');
      var expired;
      var removedCount = 0;
      var currentRemovedCount;
      do {
        currentRemovedCount = 0;
        expired = yield taskResult.getExpired(cfgExpFilesRemovedAtOnce, cfgExpFiles);
        for (var i = 0; i < expired.length; ++i) {
          var docId = expired[i].id;
          var hvals = yield docsCoServer.getAllPresencePromise(docId);
          if(0 == hvals.length){
            if (yield canvasService.cleanupCache(docId)) {
              currentRemovedCount++;
            }
          } else {
            logger.debug('checkFileExpire expire but presence: hvals = %s; docId = %s', hvals, docId);
          }
        }
        removedCount += currentRemovedCount;
      } while (currentRemovedCount > 0);
      logger.debug('checkFileExpire end: removedCount = %d', removedCount);
    } catch (e) {
      logger.error('checkFileExpire error:\r\n%s', e.stack);
    }
  });
};
var checkDocumentExpire = function() {
  return co(function* () {
    var queue = null;
    var removedCount = 0;
    var startSaveCount = 0;
    try {
      logger.debug('checkDocumentExpire start');
      var redisClient = pubsubRedis.getClientRedis();

      var now = (new Date()).getTime();
      var multi = redisClient.multi([
        ['zrangebyscore', redisKeyDocuments, 0, now],
        ['zremrangebyscore', redisKeyDocuments, 0, now]
      ]);
      var execRes = yield utils.promiseRedis(multi, multi.exec);
      var expiredKeys = execRes[0];
      if (expiredKeys.length > 0) {
        queue = new queueService();
        yield queue.initPromise(true, false, false, false, false, false);

        for (var i = 0; i < expiredKeys.length; ++i) {
          var docId = expiredKeys[i];
          if (docId) {
            var puckerIndex = yield docsCoServer.getChangesIndexPromise(docId);
            if (puckerIndex > 0) {
              yield docsCoServer.createSaveTimerPromise(docId, null, queue, true);
              startSaveCount++;
            } else {
              yield docsCoServer.cleanDocumentOnExitNoChangesPromise(docId);
              removedCount++;
            }
          }
        }
      }
    } catch (e) {
      logger.error('checkDocumentExpire error:\r\n%s', e.stack);
    } finally {
      try {
        if (queue) {
          yield queue.close();
        }
      } catch (e) {
        logger.error('checkDocumentExpire error:\r\n%s', e.stack);
      }
      logger.debug('checkDocumentExpire end: startSaveCount = %d, removedCount = %d', startSaveCount, removedCount);
    }
  });
};
let forceSaveTimeout = function() {
  return co(function* () {
    let queue = null;
    let pubsub = null;
    try {
      logger.debug('forceSaveTimeout start');
      let redisClient = pubsubRedis.getClientRedis();

      let now = (new Date()).getTime();
      let multi = redisClient.multi([
        ['zrangebyscore', redisKeyForceSaveTimer, 0, now],
        ['zremrangebyscore', redisKeyForceSaveTimer, 0, now]
      ]);
      let execRes = yield utils.promiseRedis(multi, multi.exec);
      let expiredKeys = execRes[0];
      if (expiredKeys.length > 0) {
        queue = new queueService();
        yield queue.initPromise(true, false, false, false, false, false);

        pubsub = new pubsubService();
        yield pubsub.initPromise();

        let actions = [];
        for (let i = 0; i < expiredKeys.length; ++i) {
          let docId = expiredKeys[i];
          if (docId) {
            actions.push(utils.promiseRedis(redisClient, redisClient.del, redisKeyForceSaveTimerLock + docId));
            actions.push(docsCoServer.startForceSavePromise(docId, commondefines.c_oAscForceSaveTypes.Timeout,
                                                            undefined, undefined, undefined, queue, pubsub));
          }
        }
        yield Promise.all(actions);
        logger.debug('forceSaveTimeout actions.length %d', actions.length);
      }
      logger.debug('forceSaveTimeout end');
    } catch (e) {
      logger.error('forceSaveTimeout error:\r\n%s', e.stack);
    } finally {
      try {
        if (queue) {
          yield queue.close();
        }
        if (pubsub) {
          yield pubsub.close();
        }
      } catch (e) {
        logger.error('checkDocumentExpire error:\r\n%s', e.stack);
      }
      setTimeout(forceSaveTimeout, cfgForceSaveStep);
    }
  });
};

var documentExpireJob = function(opt_isStart) {
  if (!opt_isStart) {
    logger.warn('checkDocumentExpire restart');
  }
  new cron.CronJob(cfgExpDocumentsCron, checkDocumentExpire, documentExpireJob, true);
};
documentExpireJob(true);

var fileExpireJob = function(opt_isStart) {
  if (!opt_isStart) {
    logger.warn('checkFileExpire restart');
  }
  new cron.CronJob(cfgExpFilesCron, checkFileExpire, fileExpireJob, true);
};
fileExpireJob(true);

if (cfgForceSaveEnable) {
  setTimeout(forceSaveTimeout, cfgForceSaveStep);
}
