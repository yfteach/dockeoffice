/*
 * Copyright (C) Ascensio System SIA 2012-2019. All rights reserved
 *
 * https://www.onlyoffice.com/ 
 *
 * Version: 5.3.4 (build:3)
 */


'use strict';

var pathModule = require('path');
var urlModule = require('url');
var co = require('co');
const ms = require('ms');
const retry = require('retry');
const MultiRange = require('multi-integer-range').MultiRange;
var sqlBase = require('./baseConnector');
var docsCoServer = require('./DocsCoServer');
var taskResult = require('./taskresult');
var logger = require('./../../Common/sources/logger');
var utils = require('./../../Common/sources/utils');
var constants = require('./../../Common/sources/constants');
var commonDefines = require('./../../Common/sources/commondefines');
var storage = require('./../../Common/sources/storage-base');
var formatChecker = require('./../../Common/sources/formatchecker');
var statsDClient = require('./../../Common/sources/statsdclient');
var config = require('config');
var config_server = config.get('services.CoAuthoring.server');
var config_utils = config.get('services.CoAuthoring.utils');
var pubsubRedis = require('./pubsubRedis');


var cfgTypesUpload = config_utils.get('limits_image_types_upload');
var cfgImageSize = config_server.get('limits_image_size');
var cfgImageDownloadTimeout = config_server.get('limits_image_download_timeout');
var cfgRedisPrefix = config.get('services.CoAuthoring.redis.prefix');
var cfgTokenEnableBrowser = config.get('services.CoAuthoring.token.enable.browser');
const cfgTokenEnableRequestOutbox = config.get('services.CoAuthoring.token.enable.request.outbox');
const cfgForgottenFiles = config_server.get('forgottenfiles');
const cfgForgottenFilesName = config_server.get('forgottenfilesname');
const cfgOpenProtectedFile = config_server.get('openProtectedFile');
const cfgExpUpdateVersionStatus = ms(config.get('services.CoAuthoring.expire.updateVersionStatus'));
const cfgCallbackBackoffOptions = config.get('services.CoAuthoring.callbackBackoffOptions');

var SAVE_TYPE_PART_START = 0;
var SAVE_TYPE_PART = 1;
var SAVE_TYPE_COMPLETE = 2;
var SAVE_TYPE_COMPLETE_ALL = 3;

var clientStatsD = statsDClient.getClient();
var redisClient = pubsubRedis.getClientRedis();
var redisKeySaved = cfgRedisPrefix + constants.REDIS_KEY_SAVED;
var redisKeyShutdown = cfgRedisPrefix + constants.REDIS_KEY_SHUTDOWN;

const retryHttpStatus = new MultiRange(cfgCallbackBackoffOptions.httpStatus);

function OutputDataWrap(type, data) {
  this['type'] = type;
  this['data'] = data;
}
OutputDataWrap.prototype = {
  fromObject: function(data) {
    this['type'] = data['type'];
    this['data'] = new OutputData();
    this['data'].fromObject(data['data']);
  },
  getType: function() {
    return this['type'];
  },
  setType: function(data) {
    this['type'] = data;
  },
  getData: function() {
    return this['data'];
  },
  setData: function(data) {
    this['data'] = data;
  }
};
function OutputData(type) {
  this['type'] = type;
  this['status'] = undefined;
  this['data'] = undefined;
}
OutputData.prototype = {
  fromObject: function(data) {
    this['type'] = data['type'];
    this['status'] = data['status'];
    this['data'] = data['data'];
  },
  getType: function() {
    return this['type'];
  },
  setType: function(data) {
    this['type'] = data;
  },
  getStatus: function() {
    return this['status'];
  },
  setStatus: function(data) {
    this['status'] = data;
  },
  getData: function() {
    return this['data'];
  },
  setData: function(data) {
    this['data'] = data;
  }
};

function* getOutputData(cmd, outputData, key, status, statusInfo, optConn, optAdditionalOutput, opt_bIsRestore) {
  var docId = cmd.getDocId();
  switch (status) {
    case taskResult.FileStatus.SaveVersion:
    case taskResult.FileStatus.UpdateVersion:
    case taskResult.FileStatus.Ok:
      if(taskResult.FileStatus.Ok == status) {
        outputData.setStatus('ok');
      } else if (taskResult.FileStatus.SaveVersion == status ||
        (!opt_bIsRestore && taskResult.FileStatus.UpdateVersion === status &&
        Date.now() - statusInfo * 60000 > cfgExpUpdateVersionStatus)) {
        if ((optConn && optConn.user.view) || optConn.isCloseCoAuthoring) {
          outputData.setStatus(constants.FILE_STATUS_UPDATE_VERSION);
        } else {
          if (taskResult.FileStatus.UpdateVersion === status) {
            logger.warn("UpdateVersion expired: docId = %s", docId);
          }
          var updateMask = new taskResult.TaskResultData();
          updateMask.key = docId;
          updateMask.status = status;
          updateMask.statusInfo = statusInfo;
          var updateTask = new taskResult.TaskResultData();
          updateTask.status = taskResult.FileStatus.Ok;
          updateTask.statusInfo = constants.NO_ERROR;
          var updateIfRes = yield taskResult.updateIf(updateTask, updateMask);
          if (updateIfRes.affectedRows > 0) {
            outputData.setStatus('ok');
          } else {
            outputData.setStatus(constants.FILE_STATUS_UPDATE_VERSION);
          }
        }
      } else {
        outputData.setStatus(constants.FILE_STATUS_UPDATE_VERSION);
      }
      var command = cmd.getCommand();
      if ('open' != command && 'reopen' != command && !cmd.getOutputUrls()) {
        var strPath = key + '/' + cmd.getOutputPath();
        if (optConn) {
          var contentDisposition = cmd.getInline() ? constants.CONTENT_DISPOSITION_INLINE : constants.CONTENT_DISPOSITION_ATTACHMENT;
          let url = yield storage.getSignedUrl(optConn.baseUrl, strPath, commonDefines.c_oAscUrlTypes.Temporary,
                                               cmd.getTitle(),
                                               contentDisposition);
          outputData.setData(url);
        } else if (optAdditionalOutput) {
          optAdditionalOutput.needUrlKey = strPath;
          optAdditionalOutput.needUrlMethod = 2;
          optAdditionalOutput.needUrlType = commonDefines.c_oAscUrlTypes.Temporary;
        }
      } else {
        if (optConn) {
          outputData.setData(yield storage.getSignedUrls(optConn.baseUrl, key, commonDefines.c_oAscUrlTypes.Session));
        } else if (optAdditionalOutput) {
          optAdditionalOutput.needUrlKey = key;
          optAdditionalOutput.needUrlMethod = 0;
          optAdditionalOutput.needUrlType = commonDefines.c_oAscUrlTypes.Session;
        }
      }
      break;
    case taskResult.FileStatus.NeedParams:
      outputData.setStatus('needparams');
      var settingsPath = key + '/' + 'origin.' + cmd.getFormat();
      if (optConn) {
        let url = yield storage.getSignedUrl(optConn.baseUrl, settingsPath, commonDefines.c_oAscUrlTypes.Temporary);
        outputData.setData(url);
      } else if (optAdditionalOutput) {
        optAdditionalOutput.needUrlKey = settingsPath;
        optAdditionalOutput.needUrlMethod = 1;
        optAdditionalOutput.needUrlType = commonDefines.c_oAscUrlTypes.Temporary;
      }
      break;
    case taskResult.FileStatus.NeedPassword:
      outputData.setStatus('needpassword');
      outputData.setData(statusInfo);
      break;
    case taskResult.FileStatus.Err:
    case taskResult.FileStatus.ErrToReload:
      outputData.setStatus('err');
      outputData.setData(statusInfo);
      if (taskResult.FileStatus.ErrToReload == status) {
        yield cleanupCache(key);
      }
      break;
  }
}
function* addRandomKeyTaskCmd(cmd) {
  var task = yield* taskResult.addRandomKeyTask(cmd.getDocId());
  cmd.setSaveKey(task.key);
}
function* saveParts(cmd, filename) {
  var result = false;
  var saveType = cmd.getSaveType();
  if (SAVE_TYPE_COMPLETE_ALL !== saveType) {
    let ext = pathModule.extname(filename);
    filename = pathModule.basename(filename, ext) + (cmd.getSaveIndex() || '') + ext;
  }
  if ((SAVE_TYPE_PART_START === saveType || SAVE_TYPE_COMPLETE_ALL === saveType) && !cmd.getSaveKey()) {
    yield* addRandomKeyTaskCmd(cmd);
  }
  if (cmd.getUrl()) {
    result = true;
  } else {
    var buffer = cmd.getData();
    yield storage.putObject(cmd.getSaveKey() + '/' + filename, buffer, buffer.length);
    cmd.data = null;
    result = (SAVE_TYPE_COMPLETE_ALL === saveType || SAVE_TYPE_COMPLETE === saveType);
  }
  return result;
}
function getSaveTask(cmd) {
  cmd.setData(null);
  var queueData = new commonDefines.TaskQueueData();
  queueData.setCmd(cmd);
  queueData.setToFile(constants.OUTPUT_NAME + '.' + formatChecker.getStringFromFormat(cmd.getOutputFormat()));
  return queueData;
}
function getUpdateResponse(cmd) {
  var updateTask = new taskResult.TaskResultData();
  updateTask.key = cmd.getSaveKey() ? cmd.getSaveKey() : cmd.getDocId();
  var statusInfo = cmd.getStatusInfo();
  if (constants.NO_ERROR == statusInfo) {
    updateTask.status = taskResult.FileStatus.Ok;
  } else if (constants.CONVERT_DOWNLOAD == statusInfo) {
    updateTask.status = taskResult.FileStatus.ErrToReload;
  } else if (constants.CONVERT_NEED_PARAMS == statusInfo) {
    updateTask.status = taskResult.FileStatus.NeedParams;
  } else if (constants.CONVERT_DRM == statusInfo) {
    if (cfgOpenProtectedFile) {
    updateTask.status = taskResult.FileStatus.NeedPassword;
    } else {
      updateTask.status = taskResult.FileStatus.Err;
    }
  } else if (constants.CONVERT_PASSWORD == statusInfo) {
    updateTask.status = taskResult.FileStatus.NeedPassword;
  } else if (constants.CONVERT_DEAD_LETTER == statusInfo) {
    updateTask.status = taskResult.FileStatus.ErrToReload;
  } else {
    updateTask.status = taskResult.FileStatus.Err;
  }
  updateTask.statusInfo = statusInfo;
  return updateTask;
}
var cleanupCache = co.wrap(function* (docId) {
  var res = false;
  var removeRes = yield taskResult.remove(docId);
  if (removeRes.affectedRows > 0) {
    yield storage.deletePath(docId);
    res = true;
  }
  return res;
});

function commandOpenStartPromise(docId, cmd, opt_updateUserIndex, opt_documentCallbackUrl, opt_baseUrl) {
  var task = new taskResult.TaskResultData();
  task.key = docId;
  task.status = taskResult.FileStatus.None;
  task.statusInfo = constants.NO_ERROR;
  if (opt_documentCallbackUrl && opt_baseUrl) {
    task.callback = opt_documentCallbackUrl;
    task.baseurl = opt_baseUrl;
  }
  if (!cmd) {
    logger.warn("commandOpenStartPromise empty cmd: docId = %s", docId);
  }

  return taskResult.upsert(task, opt_updateUserIndex);
}
function* commandOpen(conn, cmd, outputData, opt_upsertRes, opt_bIsRestore) {
  var upsertRes;
  if (opt_upsertRes) {
    upsertRes = opt_upsertRes;
  } else {
    upsertRes = yield commandOpenStartPromise(cmd.getDocId(), cmd);
  }
  let bCreate = upsertRes.affectedRows == 1;
  let needAddTask = bCreate;
  if (!bCreate) {
    needAddTask = yield* commandOpenFillOutput(conn, cmd, outputData, opt_bIsRestore);
  }
  if (conn.encrypted) {
    logger.debug("commandOpen encrypted %j: docId = %s", outputData, cmd.getDocId());
    if (constants.FILE_STATUS_UPDATE_VERSION !== outputData.getStatus()) {
      outputData.setStatus(undefined);
    }
  } else if (needAddTask) {
    let updateMask = new taskResult.TaskResultData();
    updateMask.key = cmd.getDocId();
    updateMask.status = taskResult.FileStatus.None;

    let task = new taskResult.TaskResultData();
    task.key = cmd.getDocId();
    task.status = taskResult.FileStatus.WaitQueue;
    task.statusInfo = constants.NO_ERROR;

    let updateIfRes = yield taskResult.updateIf(task, updateMask);
      if (updateIfRes.affectedRows > 0) {
        let forgottenId = cfgForgottenFiles + '/' + cmd.getDocId();
        let forgotten = yield storage.listObjects(forgottenId);
        if (forgotten.length > 0) {
          logger.debug("commandOpen from forgotten: docId = %s", cmd.getDocId());
          cmd.setUrl(undefined);
          cmd.setForgotten(forgottenId);
        }
        cmd.setOutputFormat(constants.AVS_OFFICESTUDIO_FILE_CANVAS);
        cmd.setEmbeddedFonts(false);
        var dataQueue = new commonDefines.TaskQueueData();
        dataQueue.setCmd(cmd);
        dataQueue.setToFile('Editor.bin');
        var priority = constants.QUEUE_PRIORITY_HIGH;
        var formatIn = formatChecker.getFormatFromString(cmd.getFormat());
        if (constants.AVS_OFFICESTUDIO_FILE_CROSSPLATFORM_PDF === formatIn ||
          constants.AVS_OFFICESTUDIO_FILE_CROSSPLATFORM_DJVU === formatIn ||
          constants.AVS_OFFICESTUDIO_FILE_CROSSPLATFORM_XPS === formatIn) {
          priority = constants.QUEUE_PRIORITY_LOW;
        }
        yield* docsCoServer.addTask(dataQueue, priority);
      } else {
        yield* commandOpenFillOutput(conn, cmd, outputData, opt_bIsRestore);
      }
    }
  }
function* commandOpenFillOutput(conn, cmd, outputData, opt_bIsRestore) {
  let needAddTask = false;
  let selectRes = yield taskResult.select(cmd.getDocId());
  if (selectRes.length > 0) {
    let row = selectRes[0];
    if (taskResult.FileStatus.None === row.status) {
      needAddTask = true;
    } else {
      yield* getOutputData(cmd, outputData, cmd.getDocId(), row.status, row.status_info, conn, undefined, opt_bIsRestore);
    }
  }
  return needAddTask;
}
function* commandReopen(cmd) {
  let res = true;
  let isPassword = undefined !== cmd.getPassword();
  if (!isPassword || cfgOpenProtectedFile) {
  let updateMask = new taskResult.TaskResultData();
  updateMask.key = cmd.getDocId();
    updateMask.status = isPassword ? taskResult.FileStatus.NeedPassword : taskResult.FileStatus.NeedParams;

  var task = new taskResult.TaskResultData();
  task.key = cmd.getDocId();
  task.status = taskResult.FileStatus.WaitQueue;
  task.statusInfo = constants.NO_ERROR;

  var upsertRes = yield taskResult.updateIf(task, updateMask);
  if (upsertRes.affectedRows > 0) {
    cmd.setUrl(null);//url may expire
    cmd.setSaveKey(cmd.getDocId());
    cmd.setOutputFormat(constants.AVS_OFFICESTUDIO_FILE_CANVAS);
    cmd.setEmbeddedFonts(false);
    var dataQueue = new commonDefines.TaskQueueData();
    dataQueue.setCmd(cmd);
    dataQueue.setToFile('Editor.bin');
    dataQueue.setFromSettings(true);
    yield* docsCoServer.addTask(dataQueue, constants.QUEUE_PRIORITY_HIGH);
  }
  } else {
    res = false;
}
  return res;
}
function* commandSave(cmd, outputData) {
  var completeParts = yield* saveParts(cmd, "Editor.bin");
  if (completeParts) {
    var queueData = getSaveTask(cmd);
    yield* docsCoServer.addTask(queueData, constants.QUEUE_PRIORITY_LOW);
  }
  outputData.setStatus('ok');
  outputData.setData(cmd.getSaveKey());
}
function* commandSendMailMerge(cmd, outputData) {
  let mailMergeSend = cmd.getMailMergeSend();
  let isJson = mailMergeSend.getIsJsonKey();
  var completeParts = yield* saveParts(cmd, isJson ? "Editor.json" : "Editor.bin");
  var isErr = false;
  if (completeParts && !isJson) {
    isErr = true;
    var getRes = yield* docsCoServer.getCallback(cmd.getDocId());
    if (getRes) {
      mailMergeSend.setUrl(getRes.server.href);
      mailMergeSend.setBaseUrl(getRes.baseUrl);
      mailMergeSend.setJsonKey(cmd.getSaveKey());
      mailMergeSend.setRecordErrorCount(0);
      yield* addRandomKeyTaskCmd(cmd);
      var queueData = getSaveTask(cmd);
      yield* docsCoServer.addTask(queueData, constants.QUEUE_PRIORITY_LOW);
      isErr = false;
    }
  }
  if (isErr) {
    outputData.setStatus('err');
    outputData.setData(constants.UNKNOWN);
  } else {
    outputData.setStatus('ok');
    outputData.setData(cmd.getSaveKey());
  }
}
function* commandSfctByCmd(cmd, opt_priority, opt_expiration, opt_queue) {
  yield* addRandomKeyTaskCmd(cmd);
  var queueData = getSaveTask(cmd);
  queueData.setFromChanges(true);
  let priority = null != opt_priority ? opt_priority : constants.QUEUE_PRIORITY_LOW;
  yield* docsCoServer.addTask(queueData, priority, opt_queue, opt_expiration);
}
function* commandSfct(cmd, outputData) {
  yield* commandSfctByCmd(cmd);
  outputData.setStatus('ok');
}
function isDisplayedImage(strName) {
  var res = 0;
  if (strName) {
    var findStr = constants.DISPLAY_PREFIX;
    var index = strName.indexOf(findStr);
    if (-1 != index) {
      if (index + findStr.length < strName.length) {
        var displayN = parseInt(strName[index + findStr.length]);
        if (!isNaN(displayN)) {
          var imageIndex = index + findStr.length + 1;
          if (imageIndex == strName.indexOf("image", imageIndex))
            res = displayN;
        }
      }
    }
  }
  return res;
}
function* commandImgurls(conn, cmd, outputData) {
  var supportedFormats = cfgTypesUpload || 'jpg';
  var urls;
  var docId = cmd.getDocId();
  var errorCode = constants.NO_ERROR;
  var outputUrls = [];
  var isImgUrl = 'imgurl' == cmd.getCommand();
  if (!conn.user.view && !conn.isCloseCoAuthoring) {
    urls = isImgUrl ? [cmd.getData()] : cmd.getData();
    var displayedImageMap = {};//to make one imageIndex for ole object urls
    var imageCount = 0;
    for (var i = 0; i < urls.length; ++i) {
      var urlSource = urls[i];
      var urlParsed;
      var data = undefined;
      if (urlSource.startsWith('data:')) {
        let delimiterIndex = urlSource.indexOf(',');
        if (-1 != delimiterIndex) {
          let dataLen = urlSource.length - (delimiterIndex + 1);
          if ('hex' === urlSource.substring(delimiterIndex - 3, delimiterIndex).toLowerCase()) {
            if (dataLen * 0.5 <= cfgImageSize) {
              data = Buffer.from(urlSource.substring(delimiterIndex + 1), 'hex');
            } else {
              errorCode = constants.UPLOAD_CONTENT_LENGTH;
            }
          } else {
            if (dataLen * 0.75 <= cfgImageSize) {
              data = Buffer.from(urlSource.substring(delimiterIndex + 1), 'base64');
            } else {
              errorCode = constants.UPLOAD_CONTENT_LENGTH;
            }
          }
        }
      } else if (urlSource) {
        try {
          let authorization;
          if (cfgTokenEnableRequestOutbox && cmd.getWithAuthorization()) {
            authorization = utils.fillJwtForRequest({url: urlSource});
          }
          data = yield utils.downloadUrlPromise(urlSource, cfgImageDownloadTimeout, cfgImageSize, authorization);
          urlParsed = urlModule.parse(urlSource);
        } catch (e) {
          data = undefined;
          logger.error('error commandImgurls download: url = %s; docId = %s\r\n%s', urlSource, docId, e.stack);
          if (e.code === 'EMSGSIZE') {
            errorCode = constants.UPLOAD_CONTENT_LENGTH;
          } else {
            errorCode = constants.UPLOAD_URL;
          }
          if (isImgUrl) {
            break;
          }
        }
      }
      var outputUrl = {url: 'error', path: 'error'};
      if (data) {
        let format = formatChecker.getImageFormat(data);
        let formatStr;
        let isAllow = false;
        if (constants.AVS_OFFICESTUDIO_FILE_UNKNOWN !== format) {
          formatStr = formatChecker.getStringFromFormat(format);
          if (formatStr && -1 !== supportedFormats.indexOf(formatStr)) {
            isAllow = true;
          } else if (!isImgUrl && 'svg' === formatStr && isDisplayedImage(pathModule.basename(urlParsed.pathname)) > 0) {
            isAllow = true;
          }
        }
        if (!isAllow && urlParsed) {
          let ext = pathModule.extname(urlParsed.pathname).substring(1);
          let urlBasename = pathModule.basename(urlParsed.pathname);
          let displayedImageName = urlBasename.substring(0, urlBasename.length - ext.length - 1);
          if (displayedImageMap.hasOwnProperty(displayedImageName)) {
            formatStr = ext;
            isAllow = true;
          }
        }
        if (isAllow) {
          var userid = cmd.getUserId();
          var imageIndex = cmd.getSaveIndex() + imageCount;
          imageCount++;
          var strLocalPath = 'media/' + utils.crc32(userid).toString(16) + '_';
          if (urlParsed) {
            var urlBasename = pathModule.basename(urlParsed.pathname);
            var displayN = isDisplayedImage(urlBasename);
            if (displayN > 0) {
              var displayedImageName = urlBasename.substring(0, urlBasename.length - formatStr.length - 1);
              var tempIndex = displayedImageMap[displayedImageName];
              if (null != tempIndex) {
                imageIndex = tempIndex;
                imageCount--;
              } else {
                displayedImageMap[displayedImageName] = imageIndex;
              }
              strLocalPath += constants.DISPLAY_PREFIX + displayN;
            }
          }
          strLocalPath += 'image' + imageIndex + '.' + formatStr;
          var strPath = cmd.getDocId() + '/' + strLocalPath;
          yield storage.putObject(strPath, data, data.length);
          var imgUrl = yield storage.getSignedUrl(conn.baseUrl, strPath, commonDefines.c_oAscUrlTypes.Session);
          outputUrl = {url: imgUrl, path: strLocalPath};
        }
      }
      if (constants.NO_ERROR === errorCode && ('error' === outputUrl.url || 'error' === outputUrl.path)) {
        errorCode = constants.UPLOAD_EXTENSION;
        if (isImgUrl) {
          break;
        }
      }
      outputUrls.push(outputUrl);
    }
  } else {
    logger.warn('error commandImgurls: docId = %s access deny', docId);
    errorCode = errorCode.UPLOAD;
  }
  if (constants.NO_ERROR !== errorCode && 0 == outputUrls.length) {
    outputData.setStatus('err');
    outputData.setData(errorCode);
  } else {
    outputData.setStatus('ok');
    if (isImgUrl) {
       outputData.setData(outputUrls);
    } else {
      outputData.setData({error: errorCode, urls: outputUrls});
    }
  }
}
function* commandPathUrls(conn, cmd, outputData) {
  let contentDisposition = cmd.getInline() ? constants.CONTENT_DISPOSITION_INLINE :
    constants.CONTENT_DISPOSITION_ATTACHMENT;
  let docId = cmd.getDocId();
  let listImages = cmd.getData().map(function callback(currentValue) {
    return docId + '/' + currentValue;
  });
  let urls = yield storage.getSignedUrlsArrayByArray(conn.baseUrl, listImages, commonDefines.c_oAscUrlTypes.Session,
                                                     contentDisposition);
  outputData.setStatus('ok');
  outputData.setData(urls);
}
function* commandPathUrl(conn, cmd, outputData) {
  var contentDisposition = cmd.getInline() ? constants.CONTENT_DISPOSITION_INLINE :
    constants.CONTENT_DISPOSITION_ATTACHMENT;
  var strPath = cmd.getDocId() + '/' + cmd.getData();
  var url = yield storage.getSignedUrl(conn.baseUrl, strPath, commonDefines.c_oAscUrlTypes.Temporary, cmd.getTitle(),
                                       contentDisposition);
  var errorCode = constants.NO_ERROR;
  if (constants.NO_ERROR !== errorCode) {
    outputData.setStatus('err');
    outputData.setData(errorCode);
  } else {
    outputData.setStatus('ok');
    outputData.setData(url);
  }
}
function* commandSaveFromOrigin(cmd, outputData) {
  yield* addRandomKeyTaskCmd(cmd);
  var queueData = getSaveTask(cmd);
  queueData.setFromOrigin(true);
  yield* docsCoServer.addTask(queueData, constants.QUEUE_PRIORITY_LOW);
  outputData.setStatus('ok');
  outputData.setData(cmd.getSaveKey());
}
function checkAuthorizationLength(authorization, data){
  let res = authorization.length < 7168;
  if (!res) {
    logger.warn('authorization too long: docId = %s; length=%d', data.getKey(), authorization.length);
    data.setChangeUrl(undefined);
    data.setChangeHistory({});
  }
  return res;
}
function* commandSfcCallback(cmd, isSfcm, isEncrypted) {
  var docId = cmd.getDocId();
  logger.debug('Start commandSfcCallback: docId = %s', docId);
  var statusInfo = cmd.getStatusInfo();
  let replyStr;
  if (constants.EDITOR_CHANGES !== statusInfo || isSfcm) {
    var saveKey = cmd.getSaveKey();
    var isError = constants.NO_ERROR != statusInfo;
    var isErrorCorrupted = constants.CONVERT_CORRUPTED == statusInfo;
    var savePathDoc = saveKey + '/' + cmd.getOutputPath();
    var savePathChanges = saveKey + '/changes.zip';
    var savePathHistory = saveKey + '/changesHistory.json';
    var getRes = yield* docsCoServer.getCallback(docId);
    var forceSave = cmd.getForceSave();
    var forceSaveType = forceSave ? forceSave.getType() : commonDefines.c_oAscForceSaveTypes.Command;
    var isSfcmSuccess = false;
    let storeForgotten = false;
    let needRetry = false;
    var statusOk;
    var statusErr;
    if (isSfcm) {
      statusOk = docsCoServer.c_oAscServerStatus.MustSaveForce;
      statusErr = docsCoServer.c_oAscServerStatus.CorruptedForce;
    } else {
      statusOk = docsCoServer.c_oAscServerStatus.MustSave;
      statusErr = docsCoServer.c_oAscServerStatus.Corrupted;
    }
    let recoverTask = new taskResult.TaskResultData();
    recoverTask.status = taskResult.FileStatus.Ok;
    recoverTask.statusInfo = constants.NO_ERROR;
    let updateIfTask = new taskResult.TaskResultData();
    updateIfTask.status = taskResult.FileStatus.UpdateVersion;
    updateIfTask.statusInfo = Math.floor(Date.now() / 60000);//minutes
    let updateIfRes;

    let updateMask = new taskResult.TaskResultData();
    updateMask.key = docId;
    let selectRes = yield taskResult.select(docId);
    let row = selectRes.length > 0 ? selectRes[0] : null;
    if (row) {
      if (isEncrypted) {
        recoverTask.status = updateMask.status = row.status;
        recoverTask.statusInfo = updateMask.statusInfo = row.status_info;
      } else if ((taskResult.FileStatus.SaveVersion === row.status && cmd.getStatusInfoIn() === row.status_info) ||
        taskResult.FileStatus.UpdateVersion === row.status) {
        if (taskResult.FileStatus.UpdateVersion === row.status) {
          updateIfRes = {affectedRows: 1};
        }
        recoverTask.status = taskResult.FileStatus.SaveVersion;
        recoverTask.statusInfo = cmd.getStatusInfoIn();
        updateMask.status = row.status;
        updateMask.statusInfo = row.status_info;
      } else {
        updateIfRes = {affectedRows: 0};
      }
    } else {
      isError = true;
    }
    if (getRes) {
      logger.debug('Callback commandSfcCallback: docId = %s callback = %s', docId, getRes.server.href);
      var outputSfc = new commonDefines.OutputSfcData();
      outputSfc.setKey(docId);
      outputSfc.setEncrypted(isEncrypted);
      var users = [];
      let isOpenFromForgotten = false;
      let userLastChangeId = cmd.getUserId() || cmd.getUserActionId();
      if (userLastChangeId) {
        users.push(userLastChangeId);
      }
      outputSfc.setUsers(users);
      if (!isSfcm) {
        var actions = [];
        var userActionId = cmd.getUserActionId() || cmd.getUserId();
        if (userActionId) {
          actions.push(new commonDefines.OutputAction(commonDefines.c_oAscUserAction.Out, userActionId));
        }
        outputSfc.setActions(actions);
      }
      outputSfc.setUserData(cmd.getUserData());
      if (!isError || isErrorCorrupted) {
        try {
          let forgottenId = cfgForgottenFiles + '/' + docId;
          let forgotten = yield storage.listObjects(forgottenId);
          let isSendHistory = 0 === forgotten.length;
          if (!isSendHistory) {
            var forgottenMarkPath = docId + '/' + cfgForgottenFilesName + '.txt';
            var forgottenMark = yield storage.listObjects(forgottenMarkPath);
            isOpenFromForgotten = 0 !== forgottenMark.length;
            isSendHistory = !isOpenFromForgotten;
            logger.debug('commandSfcCallback forgotten no empty: docId = %s isSendHistory = %s', docId, isSendHistory);
          }
          if (isSendHistory && !isEncrypted) {
            var data = yield storage.getObject(savePathHistory);
            outputSfc.setChangeHistory(JSON.parse(data.toString('utf-8')));
            let changeUrl = yield storage.getSignedUrl(getRes.baseUrl, savePathChanges,
                                                       commonDefines.c_oAscUrlTypes.Temporary);
            outputSfc.setChangeUrl(changeUrl);
          } else {
            outputSfc.setChangeHistory({});
          }
          let url = yield storage.getSignedUrl(getRes.baseUrl, savePathDoc, commonDefines.c_oAscUrlTypes.Temporary);
          outputSfc.setUrl(url);
        } catch (e) {
          logger.error('Error commandSfcCallback: docId = %s\r\n%s', docId, e.stack);
        }
        if (outputSfc.getUrl() && outputSfc.getUsers().length > 0) {
          outputSfc.setStatus(statusOk);
        } else {
          isError = true;
        }
      }
      if (isError) {
        outputSfc.setStatus(statusErr);
      }
      var uri = getRes.server.href;
      if (isSfcm) {
        let selectRes = yield taskResult.select(docId);
        let row = selectRes.length > 0 ? selectRes[0] : null;
        if (row && row.status == taskResult.FileStatus.Ok) {
          if (forceSave) {
            outputSfc.setForceSaveType(forceSaveType);
            outputSfc.setLastSave(new Date(forceSave.getTime()).toISOString());
          }
          try {
            replyStr = yield* docsCoServer.sendServerRequest(docId, uri, outputSfc, checkAuthorizationLength);
            let replyData = docsCoServer.parseReplyData(docId, replyStr);
            isSfcmSuccess = replyData && commonDefines.c_oAscServerCommandErrors.NoError == replyData.error;
          } catch (err) {
            logger.error('sendServerRequest error: docId = %s;url = %s;data = %j\r\n%s', docId, uri, outputSfc, err.stack);
          }
        }
      } else {
        let editorsCount = yield docsCoServer.getEditorsCountPromise(docId);
        logger.debug('commandSfcCallback presence: docId = %s count = %d', docId, editorsCount);
        if (0 === editorsCount || (isEncrypted && 1 === editorsCount)) {
          let lastSave = yield* docsCoServer.getLastSave(docId);
          let notModified = yield* docsCoServer.getLastForceSave(docId, lastSave);
          var lastSaveDate = lastSave ? new Date(lastSave.time) : new Date();
          outputSfc.setLastSave(lastSaveDate.toISOString());
          outputSfc.setNotModified(notModified);
          if (!updateIfRes) {
            updateIfRes = yield taskResult.updateIf(updateIfTask, updateMask);
          }
          if (updateIfRes.affectedRows > 0) {
            updateMask.status = updateIfTask.status;
            updateMask.statusInfo = updateIfTask.statusInfo;
            try {
              replyStr = yield* docsCoServer.sendServerRequest(docId, uri, outputSfc, checkAuthorizationLength);
            } catch (err) {
              logger.error('sendServerRequest error: docId = %s;url = %s;data = %j\r\n%s', docId, uri, outputSfc, err.stack);
              if (!isEncrypted && !docsCoServer.getIsShutdown() && (!err.statusCode || retryHttpStatus.has(err.statusCode.toString()))) {
                let attempt = cmd.getAttempt() || 0;
                if (attempt < cfgCallbackBackoffOptions.retries) {
                  needRetry = true;
                } else {
                  logger.debug('commandSfcCallback backoff limit exceeded: docId = %s', docId);
                }
              }
            }
            var requestRes = false;
            var replyData = docsCoServer.parseReplyData(docId, replyStr);
            if (replyData && commonDefines.c_oAscServerCommandErrors.NoError == replyData.error) {
              var multi = redisClient.multi([
                                              ['get', redisKeySaved + docId],
                                              ['del', redisKeySaved + docId]
                                            ]);
              var execRes = yield utils.promiseRedis(multi, multi.exec);
              var savedVal = execRes[0];
              requestRes = (null == savedVal || '1' === savedVal);
            }
            if (requestRes) {
              updateIfTask = undefined;
              yield docsCoServer.cleanDocumentOnExitPromise(docId, true);
              if (isOpenFromForgotten) {
                yield cleanupCache(docId);
              }
            } else {
              storeForgotten = true;
            }
          } else {
            updateIfTask = undefined;
          }
        }
      }
    } else {
      logger.warn('Empty Callback commandSfcCallback: docId = %s', docId);
      storeForgotten = true;
    }
    if (undefined !== updateIfTask && !isSfcm) {
      logger.debug('commandSfcCallback restore %d status: docId = %s', recoverTask.status, docId);
      updateIfTask.status = recoverTask.status;
      updateIfTask.statusInfo = recoverTask.statusInfo;
      updateIfRes = yield taskResult.updateIf(updateIfTask, updateMask);
      if (!(updateIfRes.affectedRows > 0)) {
        logger.debug('commandSfcCallback restore %d status failed: docId = %s', recoverTask.status, docId);
      }
    }
    if (storeForgotten && !needRetry && !isEncrypted && (!isError || isErrorCorrupted)) {
      try {
        logger.debug("storeForgotten: docId = %s", docId);
        let forgottenName = cfgForgottenFilesName + pathModule.extname(cmd.getOutputPath());
        yield storage.copyObject(savePathDoc, cfgForgottenFiles + '/' + docId + '/' + forgottenName);
      } catch (err) {
        logger.error('Error storeForgotten: docId = %s\r\n%s', docId, err.stack);
      }
    }
    if (forceSave) {
      yield* docsCoServer.setForceSave(docId, forceSave, cmd, isSfcmSuccess && !isError);
    }
    if (needRetry) {
      let attempt = cmd.getAttempt() || 0;
      cmd.setAttempt(attempt + 1);
      let queueData = new commonDefines.TaskQueueData();
      queueData.setCmd(cmd);
      let timeout = retry.createTimeout(attempt, cfgCallbackBackoffOptions.timeout);
      logger.debug('commandSfcCallback backoff timeout = %d : docId = %s', timeout, docId);
      yield* docsCoServer.addDelayed(queueData, timeout);
    }
  } else {
    logger.debug('commandSfcCallback cleanDocumentOnExitNoChangesPromise: docId = %s', docId);
    yield docsCoServer.cleanDocumentOnExitNoChangesPromise(docId, undefined, true);
  }

  if ((docsCoServer.getIsShutdown() && !isSfcm) || cmd.getRedisKey()) {
    let keyRedis = cmd.getRedisKey() ? cmd.getRedisKey() : redisKeyShutdown;
    yield utils.promiseRedis(redisClient, redisClient.srem, keyRedis, docId);
  }
  logger.debug('End commandSfcCallback: docId = %s', docId);
  return replyStr;
}
function* commandSendMMCallback(cmd) {
  var docId = cmd.getDocId();
  logger.debug('Start commandSendMMCallback: docId = %s', docId);
  var saveKey = cmd.getSaveKey();
  var statusInfo = cmd.getStatusInfo();
  var outputSfc = new commonDefines.OutputSfcData();
  outputSfc.setKey(docId);
  if (constants.NO_ERROR == statusInfo) {
    outputSfc.setStatus(docsCoServer.c_oAscServerStatus.MailMerge);
  } else {
    outputSfc.setStatus(docsCoServer.c_oAscServerStatus.Corrupted);
  }
  var mailMergeSendData = cmd.getMailMergeSend();
  var outputMailMerge = new commonDefines.OutputMailMerge(mailMergeSendData);
  outputSfc.setMailMerge(outputMailMerge);
  outputSfc.setUsers([mailMergeSendData.getUserId()]);
  var data = yield storage.getObject(saveKey + '/' + cmd.getOutputPath());
  var xml = data.toString('utf8');
  var files = xml.match(/[< ]file.*?\/>/g);
  var recordRemain = (mailMergeSendData.getRecordTo() - mailMergeSendData.getRecordFrom() + 1);
  var recordIndexStart = mailMergeSendData.getRecordCount() - recordRemain;
  for (var i = 0; i < files.length; ++i) {
    var file = files[i];
    var fieldRes = /field=["'](.*?)["']/.exec(file);
    outputMailMerge.setTo(fieldRes[1]);
    outputMailMerge.setRecordIndex(recordIndexStart + i);
    var pathRes = /path=["'](.*?)["']/.exec(file);
    var signedUrl = yield storage.getSignedUrl(mailMergeSendData.getBaseUrl(), saveKey + '/' + pathRes[1],
                                               commonDefines.c_oAscUrlTypes.Temporary);
    outputSfc.setUrl(signedUrl);
    var uri = mailMergeSendData.getUrl();
    var replyStr = null;
    try {
      replyStr = yield* docsCoServer.sendServerRequest(docId, uri, outputSfc);
    } catch (err) {
      replyStr = null;
      logger.error('sendServerRequest error: docId = %s;url = %s;data = %j\r\n%s', docId, uri, outputSfc, err.stack);
    }
    var replyData = docsCoServer.parseReplyData(docId, replyStr);
    if (!(replyData && commonDefines.c_oAscServerCommandErrors.NoError == replyData.error)) {
      var recordErrorCount = mailMergeSendData.getRecordErrorCount();
      recordErrorCount++;
      outputMailMerge.setRecordErrorCount(recordErrorCount);
      mailMergeSendData.setRecordErrorCount(recordErrorCount);
    }
  }
  var newRecordFrom = mailMergeSendData.getRecordFrom() + Math.max(files.length, 1);
  if (newRecordFrom <= mailMergeSendData.getRecordTo()) {
    mailMergeSendData.setRecordFrom(newRecordFrom);
    yield* addRandomKeyTaskCmd(cmd);
    var queueData = getSaveTask(cmd);
    yield* docsCoServer.addTask(queueData, constants.QUEUE_PRIORITY_LOW);
  } else {
    logger.debug('End MailMerge: docId = %s', docId);
  }
  logger.debug('End commandSendMMCallback: docId = %s', docId);
}

exports.openDocument = function(conn, cmd, opt_upsertRes, opt_bIsRestore) {
  return co(function* () {
    var outputData;
    var docId = conn ? conn.docId : 'null';
    try {
      var startDate = null;
      if(clientStatsD) {
        startDate = new Date();
      }
      logger.debug('Start command: docId = %s %s', docId, JSON.stringify(cmd));
      outputData = new OutputData(cmd.getCommand());
      let res = true;
      switch (cmd.getCommand()) {
        case 'open':
          yield* commandOpen(conn, cmd, outputData, opt_upsertRes, opt_bIsRestore);
          break;
        case 'reopen':
          res = yield* commandReopen(cmd);
          break;
        case 'imgurl':
        case 'imgurls':
          yield* commandImgurls(conn, cmd, outputData);
          break;
        case 'pathurl':
          yield* commandPathUrl(conn, cmd, outputData);
          break;
        case 'pathurls':
          yield* commandPathUrls(conn, cmd, outputData);
          break;
        default:
          res = false;
          break;
      }
      if(!res){
          outputData.setStatus('err');
          outputData.setData(constants.UNKNOWN);
      }
      if(clientStatsD) {
        clientStatsD.timing('coauth.openDocument.' + cmd.getCommand(), new Date() - startDate);
      }
    }
    catch (e) {
      logger.error('Error openDocument: docId = %s\r\n%s', docId, e.stack);
      if (!outputData) {
        outputData = new OutputData();
      }
      outputData.setStatus('err');
      outputData.setData(constants.UNKNOWN);
    }
    finally {
      if (outputData && outputData.getStatus()) {
        logger.debug('Response command: docId = %s %s', docId, JSON.stringify(outputData));
        docsCoServer.sendData(conn, new OutputDataWrap('documentOpen', outputData));
      }
      logger.debug('End command: docId = %s', docId);
    }
  });
};
exports.downloadAs = function(req, res) {
  return co(function* () {
    var docId = 'null';
    try {
      var startDate = null;
      if(clientStatsD) {
        startDate = new Date();
      }
      var strCmd = req.query['cmd'];
      var cmd = new commonDefines.InputCommand(JSON.parse(strCmd));
      docId = cmd.getDocId();
      logger.debug('Start downloadAs: docId = %s %s', docId, strCmd);

      if (cfgTokenEnableBrowser) {
        var isValidJwt = false;
        var checkJwtRes = docsCoServer.checkJwt(docId, cmd.getJwt(), commonDefines.c_oAscSecretType.Session);
        if (checkJwtRes.decoded) {
          var doc = checkJwtRes.decoded.document;
          if (!doc.permissions || (false !== doc.permissions.download || false !== doc.permissions.print)) {
            isValidJwt = true;
            docId = doc.key;
            cmd.setDocId(doc.key);
          } else {
            logger.warn('Error downloadAs jwt: docId = %s\r\n%s', docId, 'access deny');
          }
        } else {
          logger.warn('Error downloadAs jwt: docId = %s\r\n%s', docId, checkJwtRes.description);
        }
        if (!isValidJwt) {
          res.sendStatus(403);
          return;
        }
      }

      cmd.setData(req.body);
      var outputData = new OutputData(cmd.getCommand());
      switch (cmd.getCommand()) {
        case 'save':
          yield* commandSave(cmd, outputData);
          break;
        case 'savefromorigin':
          yield* commandSaveFromOrigin(cmd, outputData);
          break;
        case 'sendmm':
          yield* commandSendMailMerge(cmd, outputData);
          break;
        case 'sfct':
          yield* commandSfct(cmd, outputData);
          break;
        default:
          outputData.setStatus('err');
          outputData.setData(constants.UNKNOWN);
          break;
      }
      var strRes = JSON.stringify(outputData);
      res.setHeader('Content-Type', 'application/json');
      res.send(strRes);
      logger.debug('End downloadAs: docId = %s %s', docId, strRes);
      if(clientStatsD) {
        clientStatsD.timing('coauth.downloadAs.' + cmd.getCommand(), new Date() - startDate);
      }
    }
    catch (e) {
      logger.error('Error downloadAs: docId = %s\r\n%s', docId, e.stack);
      res.sendStatus(400);
    }
  });
};
exports.saveFile = function(req, res) {
  return co(function*() {
    let docId = 'null';
    try {
      let startDate = null;
      if (clientStatsD) {
        startDate = new Date();
      }

      let strCmd = req.query['cmd'];
      let cmd = new commonDefines.InputCommand(JSON.parse(strCmd));
      docId = cmd.getDocId();
      logger.debug('Start saveFile: docId = %s', docId);

      if (cfgTokenEnableBrowser) {
        let isValidJwt = false;
        let checkJwtRes = docsCoServer.checkJwt(docId, cmd.getJwt(), commonDefines.c_oAscSecretType.Session);
        if (checkJwtRes.decoded) {
          let doc = checkJwtRes.decoded.document;
          var edit = checkJwtRes.decoded.editorConfig;
          if (doc.ds_encrypted && !edit.ds_view && !edit.ds_isCloseCoAuthoring) {
            isValidJwt = true;
            docId = doc.key;
            cmd.setDocId(doc.key);
          } else {
            logger.warn('Error saveFile jwt: docId = %s\r\n%s', docId, 'access deny');
          }
        } else {
          logger.warn('Error saveFile jwt: docId = %s\r\n%s', docId, checkJwtRes.description);
        }
        if (!isValidJwt) {
          res.sendStatus(403);
          return;
        }
      }
      cmd.setStatusInfo(constants.NO_ERROR);
      yield* addRandomKeyTaskCmd(cmd);
      yield storage.putObject(cmd.getSaveKey() + '/' + cmd.getOutputPath(), req.body, req.body.length);
      let replyStr = yield* commandSfcCallback(cmd, false, true);
      if (replyStr) {
        utils.fillResponseSimple(res, replyStr, 'application/json');
      } else {
        res.sendStatus(400);
      }
      logger.debug('End saveFile: docId = %s %s', docId, replyStr);
      if (clientStatsD) {
        clientStatsD.timing('coauth.saveFile', new Date() - startDate);
      }
    }
    catch (e) {
      logger.error('Error saveFile: docId = %s\r\n%s', docId, e.stack);
      res.sendStatus(400);
    }
  });
};
exports.saveFromChanges = function(docId, statusInfo, optFormat, opt_userId, opt_queue) {
  return co(function* () {
    try {
      var startDate = null;
      if(clientStatsD) {
        startDate = new Date();
      }
      logger.debug('Start saveFromChanges: docId = %s', docId);
      var task = new taskResult.TaskResultData();
      task.key = docId;
      var selectRes = yield taskResult.select(docId);
      var row = selectRes.length > 0 ? selectRes[0] : null;
      if (row && row.status == taskResult.FileStatus.SaveVersion && row.status_info == statusInfo) {
        if (null == optFormat) {
          optFormat = constants.AVS_OFFICESTUDIO_FILE_OTHER_TEAMLAB_INNER;
        }
        var cmd = new commonDefines.InputCommand();
        cmd.setCommand('sfc');
        cmd.setDocId(docId);
        cmd.setOutputFormat(optFormat);
        cmd.setStatusInfoIn(statusInfo);
        cmd.setUserActionId(opt_userId);
        yield* addRandomKeyTaskCmd(cmd);
        var queueData = getSaveTask(cmd);
        queueData.setFromChanges(true);
        yield* docsCoServer.addTask(queueData, constants.QUEUE_PRIORITY_NORMAL, opt_queue);
        if (docsCoServer.getIsShutdown()) {
          yield utils.promiseRedis(redisClient, redisClient.sadd, redisKeyShutdown, docId);
        }
        logger.debug('AddTask saveFromChanges: docId = %s', docId);
      } else {
        if (row) {
          logger.debug('saveFromChanges status mismatch: docId = %s; row: %d; %d; expected: %d', docId, row.status, row.status_info, statusInfo);
        }
      }
      if (clientStatsD) {
        clientStatsD.timing('coauth.saveFromChanges', new Date() - startDate);
      }
    }
    catch (e) {
      logger.error('Error saveFromChanges: docId = %s\r\n%s', docId, e.stack);
    }
  });
};
exports.receiveTask = function(data) {
  return co(function* () {
    var docId = 'null';
    try {
      var task = new commonDefines.TaskQueueData(JSON.parse(data));
      if (task) {
        var cmd = task.getCmd();
        docId = cmd.getDocId();
        logger.debug('Start receiveTask: docId = %s %s', docId, data);
        var updateTask = getUpdateResponse(cmd);
        var updateRes = yield taskResult.update(updateTask);
        if (updateRes.affectedRows > 0) {
          var outputData = new OutputData(cmd.getCommand());
          var command = cmd.getCommand();
          var additionalOutput = {needUrlKey: null, needUrlMethod: null, needUrlType: null};
          if ('open' == command || 'reopen' == command) {
            yield* getOutputData(cmd, outputData, cmd.getDocId(), updateTask.status,
              updateTask.statusInfo, null, additionalOutput);
          } else if ('save' == command || 'savefromorigin' == command || 'sfct' == command) {
            yield* getOutputData(cmd, outputData, cmd.getSaveKey(), updateTask.status,
              updateTask.statusInfo, null, additionalOutput);
          } else if ('sfcm' == command) {
            yield* commandSfcCallback(cmd, true);
          } else if ('sfc' == command) {
            yield* commandSfcCallback(cmd, false);
          } else if ('sendmm' == command) {
            yield* commandSendMMCallback(cmd);
          } else if ('conv' == command) {
          }
          if (outputData.getStatus()) {
            logger.debug('Send receiveTask: docId = %s %s', docId, JSON.stringify(outputData));
            var output = new OutputDataWrap('documentOpen', outputData);
            yield* docsCoServer.publish({
                                          type: commonDefines.c_oPublishType.receiveTask, cmd: cmd, output: output,
                                          needUrlKey: additionalOutput.needUrlKey,
                                          needUrlMethod: additionalOutput.needUrlMethod,
                                          needUrlType: additionalOutput.needUrlType
                                        });
          }
        }
        logger.debug('End receiveTask: docId = %s', docId);
      }
    } catch (err) {
      logger.debug('Error receiveTask: docId = %s\r\n%s', docId, err.stack);
    }
  });
};

exports.cleanupCache = cleanupCache;
exports.commandSfctByCmd = commandSfctByCmd;
exports.commandOpenStartPromise = commandOpenStartPromise;
exports.OutputDataWrap = OutputDataWrap;
exports.OutputData = OutputData;
