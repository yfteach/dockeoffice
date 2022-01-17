/*
 * Copyright (C) Ascensio System SIA 2012-2019. All rights reserved
 *
 * https://www.onlyoffice.com/ 
 *
 * Version: 5.3.4 (build:3)
 */


'use strict';
var os = require('os');
var path = require('path');
var fs = require('fs');
var url = require('url');
var childProcess = require('child_process');
var co = require('co');
var config = require('config');
var spawnAsync = require('@expo/spawn-async');
const bytes = require('bytes');
var configConverter = config.get('FileConverter.converter');

var commonDefines = require('./../../Common/sources/commondefines');
var storage = require('./../../Common/sources/storage-base');
var utils = require('./../../Common/sources/utils');
var logger = require('./../../Common/sources/logger');
var constants = require('./../../Common/sources/constants');
var baseConnector = require('./../../DocService/sources/baseConnector');
var statsDClient = require('./../../Common/sources/statsdclient');
var queueService = require('./../../Common/sources/taskqueueRabbitMQ');

var cfgDownloadMaxBytes = configConverter.has('maxDownloadBytes') ? configConverter.get('maxDownloadBytes') : 100000000;
var cfgDownloadTimeout = configConverter.has('downloadTimeout') ? configConverter.get('downloadTimeout') : 60;
var cfgDownloadAttemptMaxCount = configConverter.has('downloadAttemptMaxCount') ? configConverter.get('downloadAttemptMaxCount') : 3;
var cfgDownloadAttemptDelay = configConverter.has('downloadAttemptDelay') ? configConverter.get('downloadAttemptDelay') : 1000;
var cfgFontDir = configConverter.get('fontDir');
var cfgPresentationThemesDir = configConverter.get('presentationThemesDir');
var cfgX2tPath = configConverter.get('x2tPath');
var cfgDocbuilderPath = configConverter.get('docbuilderPath');
var cfgDocbuilderAllFontsPath = configConverter.get('docbuilderAllFontsPath');
var cfgArgs = configConverter.get('args');
var cfgSpawnOptions = configConverter.get('spawnOptions');
if (cfgSpawnOptions.env) {
  Object.assign(cfgSpawnOptions.env, process.env);
}
var cfgErrorFiles = configConverter.get('errorfiles');
var cfgInputLimits = configConverter.get('inputLimits');
const cfgStreamWriterBufferSize = configConverter.get('streamWriterBufferSize');
const cfgMaxRequestChanges = config.get('services.CoAuthoring.server.maxRequestChanges');
var cfgTokenEnableRequestOutbox = config.get('services.CoAuthoring.token.enable.request.outbox');
const cfgForgottenFilesName = config.get('services.CoAuthoring.server.forgottenfilesname');
var MAX_OPEN_FILES = 200;
var TEMP_PREFIX = 'ASC_CONVERT';
var queue = null;
var clientStatsD = statsDClient.getClient();
var exitCodesReturn = [constants.CONVERT_NEED_PARAMS, constants.CONVERT_CORRUPTED, constants.CONVERT_DRM,
  constants.CONVERT_PASSWORD, constants.CONVERT_LIMITS];
var exitCodesMinorError = [constants.CONVERT_NEED_PARAMS, constants.CONVERT_DRM, constants.CONVERT_PASSWORD];
var exitCodesUpload = [constants.NO_ERROR, constants.CONVERT_CORRUPTED, constants.CONVERT_NEED_PARAMS,
  constants.CONVERT_DRM];
let inputLimitsXmlCache;

function TaskQueueDataConvert(task) {
  var cmd = task.getCmd();
  this.key = cmd.savekey ? cmd.savekey : cmd.id;
  this.fileFrom = null;
  this.fileTo = null;
  if(constants.AVS_OFFICESTUDIO_FILE_OTHER_PDFA !== cmd.outputformat){
    this.formatTo = cmd.outputformat;
  } else {
    this.formatTo = constants.AVS_OFFICESTUDIO_FILE_CROSSPLATFORM_PDF;
    this.isPDFA = true;
  }
  this.csvTxtEncoding = cmd.getCodepage();
  this.csvDelimiter = cmd.getDelimiter();
  this.csvDelimiterChar = cmd.getDelimiterChar();
  this.paid = task.getPaid();
  this.embeddedFonts = cmd.embeddedfonts;
  this.fromChanges = task.getFromChanges();
  if (cfgFontDir) {
    this.fontDir = path.resolve(cfgFontDir);
  } else {
    this.fontDir = cfgFontDir;
  }
  this.themeDir = path.resolve(cfgPresentationThemesDir);
  this.mailMergeSend = cmd.mailmergesend;
  this.thumbnail = cmd.thumbnail;
  this.doctParams = cmd.getDoctParams();
  this.password = cmd.getPassword();
  this.noBase64 = cmd.getNoBase64();
  this.timestamp = new Date();
}
TaskQueueDataConvert.prototype = {
  serialize: function(fsPath) {
    let xml = '\ufeff<?xml version="1.0" encoding="utf-8"?>';
    xml += '<TaskQueueDataConvert xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"';
    xml += ' xmlns:xsd="http://www.w3.org/2001/XMLSchema">';
    xml += this.serializeXmlProp('m_sKey', this.key);
    xml += this.serializeXmlProp('m_sFileFrom', this.fileFrom);
    xml += this.serializeXmlProp('m_sFileTo', this.fileTo);
    xml += this.serializeXmlProp('m_nFormatTo', this.formatTo);
    xml += this.serializeXmlProp('m_bIsPDFA', this.isPDFA);
    xml += this.serializeXmlProp('m_nCsvTxtEncoding', this.csvTxtEncoding);
    xml += this.serializeXmlProp('m_nCsvDelimiter', this.csvDelimiter);
    xml += this.serializeXmlProp('m_nCsvDelimiterChar', this.csvDelimiterChar);
    xml += this.serializeXmlProp('m_bPaid', this.paid);
    xml += this.serializeXmlProp('m_bEmbeddedFonts', this.embeddedFonts);
    xml += this.serializeXmlProp('m_bFromChanges', this.fromChanges);
    xml += this.serializeXmlProp('m_sFontDir', this.fontDir);
    xml += this.serializeXmlProp('m_sThemeDir', this.themeDir);
    if (this.mailMergeSend) {
      xml += this.serializeMailMerge(this.mailMergeSend);
    }
    if (this.thumbnail) {
      xml += this.serializeThumbnail(this.thumbnail);
    }
    xml += this.serializeXmlProp('m_nDoctParams', this.doctParams);
    xml += this.serializeXmlProp('m_oTimestamp', this.timestamp.toISOString());
    xml += this.serializeXmlProp('m_bIsNoBase64', this.noBase64);
    xml += this.serializeLimit();
    xml += '</TaskQueueDataConvert>';
    fs.writeFileSync(fsPath, xml, {encoding: 'utf8'});
    let hiddenXml;
    if (undefined !== this.password) {
      hiddenXml = '<TaskQueueDataConvert>';
      hiddenXml += this.serializeXmlProp('m_sPassword', this.password);
      hiddenXml += '</TaskQueueDataConvert>';
    }
    return hiddenXml;
  },
  serializeMailMerge: function(data) {
    var xml = '<m_oMailMergeSend>';
    xml += this.serializeXmlProp('from', data.getFrom());
    xml += this.serializeXmlProp('to', data.getTo());
    xml += this.serializeXmlProp('subject', data.getSubject());
    xml += this.serializeXmlProp('mailFormat', data.getMailFormat());
    xml += this.serializeXmlProp('fileName', data.getFileName());
    xml += this.serializeXmlProp('message', data.getMessage());
    xml += this.serializeXmlProp('recordFrom', data.getRecordFrom());
    xml += this.serializeXmlProp('recordTo', data.getRecordTo());
    xml += this.serializeXmlProp('recordCount', data.getRecordCount());
    xml += this.serializeXmlProp('userid', data.getUserId());
    xml += this.serializeXmlProp('url', data.getUrl());
    xml += '</m_oMailMergeSend>';
    return xml;
  },
  serializeThumbnail: function(data) {
    var xml = '<m_oThumbnail>';
    xml += this.serializeXmlProp('format', data.getFormat());
    xml += this.serializeXmlProp('aspect', data.getAspect());
    xml += this.serializeXmlProp('first', data.getFirst());
    xml += this.serializeXmlProp('width', data.getWidth());
    xml += this.serializeXmlProp('height', data.getHeight());
    xml += '</m_oThumbnail>';
    return xml;
  },
  serializeLimit: function() {
    if (!inputLimitsXmlCache) {
      var xml = '<m_oInputLimits>';
      for (let i = 0; i < cfgInputLimits.length; ++i) {
        let limit = cfgInputLimits[i];
        if (limit.type && limit.zip) {
          xml += '<m_oInputLimit';
          xml += this.serializeXmlAttr('type', limit.type);
          xml += '>';
          xml += '<m_oZip';
          if (limit.zip.compressed) {
            xml += this.serializeXmlAttr('compressed', bytes.parse(limit.zip.compressed));
          }
          if (limit.zip.uncompressed) {
            xml += this.serializeXmlAttr('uncompressed', bytes.parse(limit.zip.uncompressed));
          }
          xml += this.serializeXmlAttr('template', limit.zip.template);
          xml += '/>';
          xml += '</m_oInputLimit>';
        }
      }
      xml += '</m_oInputLimits>';
      inputLimitsXmlCache = xml;
    }
    return inputLimitsXmlCache;
  },
  serializeXmlProp: function(name, value) {
    var xml = '';
    if (null != value) {
      xml += '<' + name + '>';
      xml += utils.encodeXml(value.toString());
      xml += '</' + name + '>';
    } else {
      xml += '<' + name + ' xsi:nil="true" />';
    }
    return xml;
  },
  serializeXmlAttr: function(name, value) {
    var xml = '';
    if (null != value) {
      xml += ' ' + name + '=\"';
      xml += utils.encodeXml(value.toString());
      xml += '\"';
    }
    return xml;
  }
};

function getTempDir() {
  var tempDir = os.tmpdir();
  var now = new Date();
  var newTemp;
  while (!newTemp || fs.existsSync(newTemp)) {
    var newName = [TEMP_PREFIX, now.getYear(), now.getMonth(), now.getDate(),
      '-', (Math.random() * 0x100000000 + 1).toString(36)
    ].join('');
    newTemp = path.join(tempDir, newName);
  }
  fs.mkdirSync(newTemp);
  var sourceDir = path.join(newTemp, 'source');
  fs.mkdirSync(sourceDir);
  var resultDir = path.join(newTemp, 'result');
  fs.mkdirSync(resultDir);
  return {temp: newTemp, source: sourceDir, result: resultDir};
}
function* downloadFile(docId, uri, fileFrom) {
  var res = false;
  var data = null;
  var downloadAttemptCount = 0;
  var urlParsed = url.parse(uri);
  var filterStatus = yield* utils.checkHostFilter(urlParsed.hostname);
  if (0 == filterStatus) {
    while (!res && downloadAttemptCount++ < cfgDownloadAttemptMaxCount) {
      try {
        let authorization;
        if (cfgTokenEnableRequestOutbox) {
          authorization = utils.fillJwtForRequest({url: uri});
        }
        data = yield utils.downloadUrlPromise(uri, cfgDownloadTimeout, cfgDownloadMaxBytes, authorization);
        res = true;
      } catch (err) {
        res = false;
        logger.error('error downloadFile:url=%s;attempt=%d;code:%s;connect:%s;(id=%s)\r\n%s', uri, downloadAttemptCount, err.code, err.connect, docId, err.stack);
        if (err.code === 'ETIMEDOUT' || err.code === 'ESOCKETTIMEDOUT' || err.code === 'EMSGSIZE') {
          break;
        } else {
          yield utils.sleep(cfgDownloadAttemptDelay);
        }
      }
    }
    if (res) {
      logger.debug('downloadFile complete filesize=%d (id=%s)', data.length, docId);
      fs.writeFileSync(fileFrom, data);
    }
  } else {
    logger.error('checkIpFilter error:url=%s;code:%s;(id=%s)', uri, filterStatus, docId);
    res = false;
  }
  return res;
}
function* downloadFileFromStorage(id, strPath, dir) {
  var list = yield storage.listObjects(strPath);
  logger.debug('downloadFileFromStorage list %s (id=%s)', list.toString(), id);
  var dirsToCreate = [];
  var dirStruct = {};
  list.forEach(function(file) {
    var curDirPath = dir;
    var curDirStruct = dirStruct;
    var parts = storage.getRelativePath(strPath, file).split('/');
    for (var i = 0; i < parts.length - 1; ++i) {
      var part = parts[i];
      curDirPath = path.join(curDirPath, part);
      if (!curDirStruct[part]) {
        curDirStruct[part] = {};
        dirsToCreate.push(curDirPath);
      }
    }
  });
  for (var i = 0; i < dirsToCreate.length; ++i) {
    fs.mkdirSync(dirsToCreate[i]);
  }
  for (var i = 0; i < list.length; ++i) {
    var file = list[i];
    var fileRel = storage.getRelativePath(strPath, file);
    var data = yield storage.getObject(file);
    fs.writeFileSync(path.join(dir, fileRel), data);
  }
}
function* processDownloadFromStorage(dataConvert, cmd, task, tempDirs, authorProps) {
  let res = constants.NO_ERROR;
  let needConcatFiles = false;
  if (task.getFromOrigin() || task.getFromSettings()) {
    dataConvert.fileFrom = path.join(tempDirs.source, 'origin.' + cmd.getFormat());
  } else {
    yield* downloadFileFromStorage(cmd.getSaveKey(), cmd.getSaveKey(), tempDirs.source);
    dataConvert.fileFrom = path.join(tempDirs.source, 'Editor.bin');
    needConcatFiles = true;
  }
  let mailMergeSend = cmd.getMailMergeSend();
  if (mailMergeSend) {
    yield* downloadFileFromStorage(mailMergeSend.getJsonKey(), mailMergeSend.getJsonKey(), tempDirs.source);
    needConcatFiles = true;
  }
  if (needConcatFiles) {
    yield* concatFiles(tempDirs.source);
  }
  if (task.getFromChanges()) {
    res = yield* processChanges(tempDirs, cmd, authorProps);
  }
  return res;
}

function* concatFiles(source) {
  let list = yield utils.listObjects(source, true);
  list.sort(utils.compareStringByLength);
  let writeStreams = {};
  for (let i = 0; i < list.length; ++i) {
    let file = list[i];
    if (file.match(/Editor\d+\./)) {
      let target = file.replace(/(Editor)\d+(\..*)/, '$1$2');
      let writeStream = writeStreams[target];
      if (!writeStream) {
        writeStream = yield utils.promiseCreateWriteStream(target);
        writeStreams[target] = writeStream;
      }
      let readStream = yield utils.promiseCreateReadStream(file);
      yield utils.pipeStreams(readStream, writeStream, false);
    }
  }
  for (let i in writeStreams) {
    if (writeStreams.hasOwnProperty(i)) {
      writeStreams[i].end();
    }
  }
}

function* processChanges(tempDirs, cmd, authorProps) {
  let res = constants.NO_ERROR;
  let changesDir = path.join(tempDirs.source, constants.CHANGES_NAME);
  fs.mkdirSync(changesDir);
  let indexFile = 0;
  let changesAuthor = null;
  let changesHistory = {
    serverVersion: commonDefines.buildVersion,
    changes: []
  };
  let forceSave = cmd.getForceSave();
  let forceSaveTime;
  let forceSaveIndex = Number.MAX_VALUE;
  if (forceSave) {
    forceSaveTime = forceSave.getTime();
    forceSaveIndex = forceSave.getIndex();
  }
  let streamObj = yield* streamCreate(cmd.getDocId(), changesDir, indexFile++, {highWaterMark: cfgStreamWriterBufferSize});
  let curIndexStart = 0;
  let curIndexEnd = Math.min(curIndexStart + cfgMaxRequestChanges, forceSaveIndex);
  while (curIndexStart < curIndexEnd) {
    let changes = yield baseConnector.getChangesPromise(cmd.getDocId(), curIndexStart, curIndexEnd, forceSaveTime);
    for (let i = 0; i < changes.length; ++i) {
      let change = changes[i];
      if (change.change_data.startsWith('ENCRYPTED;')) {
        logger.warn('processChanges encrypted changes (id=%s)', cmd.getDocId());
        res = constants.EDITOR_CHANGES;
        break;
      }
      if (null === changesAuthor || changesAuthor !== change.user_id_original) {
        if (null !== changesAuthor) {
          yield* streamEnd(streamObj, ']');
          streamObj = yield* streamCreate(cmd.getDocId(), changesDir, indexFile++);
        }
        changesAuthor = change.user_id_original;
        authorProps.lastModifiedBy = change.user_name;
        authorProps.modified = change.change_date.toISOString().slice(0, 19) + 'Z';
        let strDate = baseConnector.getDateTime(change.change_date);
        changesHistory.changes.push({'created': strDate, 'user': {'id': changesAuthor, 'name': change.user_name}});
        yield* streamWrite(streamObj, '[');
      } else {
        yield* streamWrite(streamObj, ',');
      }
      yield* streamWrite(streamObj, change.change_data);
      streamObj.isNoChangesInFile = false;
    }
    if (changes.length === curIndexEnd - curIndexStart) {
      curIndexStart += cfgMaxRequestChanges;
      curIndexEnd = Math.min(curIndexStart + cfgMaxRequestChanges, forceSaveIndex);
    } else {
      break;
    }
  }
  yield* streamEnd(streamObj, ']');
  if (streamObj.isNoChangesInFile) {
    fs.unlinkSync(streamObj.filePath);
  }
  cmd.setUserId(changesAuthor);
  fs.writeFileSync(path.join(tempDirs.result, 'changesHistory.json'), JSON.stringify(changesHistory), 'utf8');
  return res;
}

function* streamCreate(docId, changesDir, indexFile, opt_options) {
  let fileName = constants.CHANGES_NAME + indexFile + '.json';
  let filePath = path.join(changesDir, fileName);
  let writeStream = yield utils.promiseCreateWriteStream(filePath, opt_options);
  writeStream.on('error', function(err) {
    logger.error('WriteStreamError (id=%s)\r\n%s', docId, err.stack);
  });
  return {writeStream: writeStream, filePath: filePath, isNoChangesInFile: true};
}

function* streamWrite(streamObj, text) {
  if (!streamObj.writeStream.write(text, 'utf8')) {
    yield utils.promiseWaitDrain(streamObj.writeStream);
  }
}

function* streamEnd(streamObj, text) {
  streamObj.writeStream.end(text, 'utf8');
  yield utils.promiseWaitClose(streamObj.writeStream);
}
function* processUploadToStorage(dir, storagePath) {
  var list = yield utils.listObjects(dir);
  if (list.length < MAX_OPEN_FILES) {
    yield* processUploadToStorageChunk(list, dir, storagePath);
  } else {
    for (var i = 0, j = list.length; i < j; i += MAX_OPEN_FILES) {
      yield* processUploadToStorageChunk(list.slice(i, i + MAX_OPEN_FILES), dir, storagePath);
    }
  }
}
function* processUploadToStorageChunk(list, dir, storagePath) {
  yield Promise.all(list.map(function (curValue) {
    let localValue = storagePath + '/' + curValue.substring(dir.length + 1);
    return storage.uploadObject(localValue, curValue);
  }));
}
function writeProcessOutputToLog(docId, childRes, isDebug) {
  if (childRes) {
    if (undefined !== childRes.stdout) {
      if (isDebug) {
        logger.debug('stdout (id=%s):%s', docId, childRes.stdout);
      } else {
        logger.error('stdout (id=%s):%s', docId, childRes.stdout);
      }
    }
    if (undefined !== childRes.stderr) {
      if (isDebug) {
        logger.debug('stderr (id=%s):%s', docId, childRes.stderr);
      } else {
        logger.error('stderr (id=%s):%s', docId, childRes.stderr);
      }
    }
  }
}
function* postProcess(cmd, dataConvert, tempDirs, childRes, error, isTimeout) {
  var exitCode = 0;
  var exitSignal = null;
  if(childRes) {
    exitCode = childRes.status;
    exitSignal = childRes.signal;
  }
  if (0 !== exitCode || null !== exitSignal) {
    if (-1 !== exitCodesReturn.indexOf(-exitCode)) {
      error = -exitCode;
    } else if(isTimeout) {
      error = constants.CONVERT_TIMEOUT;
    } else {
      error = constants.CONVERT;
    }
    if (-1 !== exitCodesMinorError.indexOf(error)) {
      writeProcessOutputToLog(dataConvert.key, childRes, true);
      logger.debug('ExitCode (code=%d;signal=%s;error:%d;id=%s)', exitCode, exitSignal, error, dataConvert.key);
    } else {
      writeProcessOutputToLog(dataConvert.key, childRes, false);
      logger.error('ExitCode (code=%d;signal=%s;error:%d;id=%s)', exitCode, exitSignal, error, dataConvert.key);
      if (cfgErrorFiles) {
        yield* processUploadToStorage(tempDirs.temp, cfgErrorFiles + '/' + dataConvert.key);
        logger.debug('processUploadToStorage error complete(id=%s)', dataConvert.key);
      }
    }
  } else {
    writeProcessOutputToLog(dataConvert.key, childRes, true);
    logger.debug('ExitCode (code=%d;signal=%s;error:%d;id=%s)', exitCode, exitSignal, error, dataConvert.key);
  }
  if (-1 !== exitCodesUpload.indexOf(error)) {
    yield* processUploadToStorage(tempDirs.result, dataConvert.key);
    logger.debug('processUploadToStorage complete(id=%s)', dataConvert.key);
  }
  cmd.setStatusInfo(error);
  var existFile = false;
  try {
    existFile = fs.lstatSync(dataConvert.fileTo).isFile();
  } catch (err) {
    existFile = false;
  }
  if (!existFile) {
    var fileToBasename = path.basename(dataConvert.fileTo);
    var fileToDir = path.dirname(dataConvert.fileTo);
    var files = fs.readdirSync(fileToDir);
    for (var i = 0; i < files.length; ++i) {
      var fileCur = files[i];
      if (0 == fileCur.indexOf(fileToBasename)) {
        dataConvert.fileTo = path.join(fileToDir, fileCur);
        break;
      }
    }
  }
  cmd.setOutputPath(path.basename(dataConvert.fileTo));
  if(!cmd.getTitle()){
    cmd.setTitle(cmd.getOutputPath());
  }

  var res = new commonDefines.TaskQueueData();
  res.setCmd(cmd);
  logger.debug('output (data=%s;id=%s)', JSON.stringify(res), dataConvert.key);
  return res;
}
function deleteFolderRecursive(strPath) {
  if (fs.existsSync(strPath)) {
    var files = fs.readdirSync(strPath);
    files.forEach(function(file) {
      var curPath = path.join(strPath, file);
      if (fs.lstatSync(curPath).isDirectory()) { // recurse
        deleteFolderRecursive(curPath);
      } else { // delete file
        fs.unlinkSync(curPath);
      }
    });
    fs.rmdirSync(strPath);
  }
}

function* ExecuteTask(task) {
  var startDate = null;
  var curDate = null;
  if(clientStatsD) {
    startDate = curDate = new Date();
  }
  var resData;
  var tempDirs;
  var getTaskTime = new Date();
  var cmd = task.getCmd();
  var dataConvert = new TaskQueueDataConvert(task);
  logger.debug('Start Task(id=%s)', dataConvert.key);
  var error = constants.NO_ERROR;
  tempDirs = getTempDir();
  let fileTo = task.getToFile();
  dataConvert.fileTo = fileTo ? path.join(tempDirs.result, fileTo) : '';
  let isBuilder = cmd.getIsBuilder();
  let authorProps = {lastModifiedBy: null, modified: null};
  if (cmd.getUrl()) {
    dataConvert.fileFrom = path.join(tempDirs.source, dataConvert.key + '.' + cmd.getFormat());
    var isDownload = yield* downloadFile(dataConvert.key, cmd.getUrl(), dataConvert.fileFrom);
    if (!isDownload) {
      error = constants.CONVERT_DOWNLOAD;
    }
    if(clientStatsD) {
      clientStatsD.timing('conv.downloadFile', new Date() - curDate);
      curDate = new Date();
    }
  } else if (cmd.getSaveKey()) {
    yield* downloadFileFromStorage(cmd.getDocId(), cmd.getDocId(), tempDirs.source);
    logger.debug('downloadFileFromStorage complete(id=%s)', dataConvert.key);
    if(clientStatsD) {
      clientStatsD.timing('conv.downloadFileFromStorage', new Date() - curDate);
      curDate = new Date();
    }
    error = yield* processDownloadFromStorage(dataConvert, cmd, task, tempDirs, authorProps);
  } else if (cmd.getForgotten()) {
    yield* downloadFileFromStorage(cmd.getDocId(), cmd.getForgotten(), tempDirs.source);
    logger.debug('downloadFileFromStorage complete(id=%s)', dataConvert.key);
    let list = yield utils.listObjects(tempDirs.source, false);
    if (list.length > 0) {
      dataConvert.fileFrom = list[0];
      var forgottenMarkPath = tempDirs.result + '/' + cfgForgottenFilesName + '.txt';
      fs.writeFileSync(forgottenMarkPath, cfgForgottenFilesName, {encoding: 'utf8'});
    } else {
      error = constants.UNKNOWN;
    }
  } else if (isBuilder) {
    yield* downloadFileFromStorage(cmd.getDocId(), cmd.getDocId(), tempDirs.source);
    logger.debug('downloadFileFromStorage complete(id=%s)', dataConvert.key);
    let list = yield utils.listObjects(tempDirs.source, false);
    if (list.length > 0) {
      dataConvert.fileFrom = list[0];
    }
  } else {
    error = constants.UNKNOWN;
  }
  var childRes = null;
  let isTimeout = false;
  if (constants.NO_ERROR === error) {
    if(constants.AVS_OFFICESTUDIO_FILE_OTHER_HTMLZIP === dataConvert.formatTo && cmd.getSaveKey() && !dataConvert.mailMergeSend) {
      yield utils.pipeFiles(dataConvert.fileFrom, dataConvert.fileTo);
    } else {
      var childArgs;
      if (cfgArgs.length > 0) {
        childArgs = cfgArgs.trim().replace(/  +/g, ' ').split(' ');
      } else {
        childArgs = [];
      }
      let processPath;
      if (!isBuilder) {
        processPath = cfgX2tPath;
        let paramsFile = path.join(tempDirs.temp, 'params.xml');
        let hiddenXml = dataConvert.serialize(paramsFile);
        childArgs.push(paramsFile);
        if (hiddenXml) {
          childArgs.push(hiddenXml);
        }
      } else {
        fs.mkdirSync(path.join(tempDirs.result, 'output'));
        processPath = cfgDocbuilderPath;
        childArgs.push('--all-fonts-path=' + cfgDocbuilderAllFontsPath);
        childArgs.push('--save-use-only-names=' + tempDirs.result + '/output');
        childArgs.push(dataConvert.fileFrom);
      }
      let timeoutId;
      try {
        let spawnOptions = cfgSpawnOptions;
        if (authorProps.lastModifiedBy && authorProps.modified) {
          spawnOptions = Object.assign({}, cfgSpawnOptions);
          spawnOptions.env = Object.assign({}, spawnOptions.env || process.env);

          spawnOptions.env['LAST_MODIFIED_BY'] = authorProps.lastModifiedBy;
          spawnOptions.env['MODIFIED'] = authorProps.modified;
        }
        let spawnAsyncPromise = spawnAsync(processPath, childArgs, spawnOptions);
        childRes = spawnAsyncPromise.child;
        let waitMS = task.getVisibilityTimeout() * 1000 - (new Date().getTime() - getTaskTime.getTime());
        timeoutId = setTimeout(function() {
          isTimeout = true;
          timeoutId = undefined;
          childRes.stdin.end();
          childRes.stdout.destroy();
          childRes.stderr.destroy();
          childRes.kill();
        }, waitMS);
        childRes = yield spawnAsyncPromise;
      } catch (err) {
        let fLog = null === err.status ? logger.error : logger.debug;
        fLog.call(logger, 'error spawnAsync(id=%s)\r\n%s', cmd.getDocId(), err.stack);
        childRes = err;
      }
      if (undefined !== timeoutId) {
        clearTimeout(timeoutId);
      }
    }
    if(clientStatsD) {
      clientStatsD.timing('conv.spawnSync', new Date() - curDate);
      curDate = new Date();
    }
  }
  resData = yield* postProcess(cmd, dataConvert, tempDirs, childRes, error, isTimeout);
  logger.debug('postProcess (id=%s)', dataConvert.key);
  if(clientStatsD) {
    clientStatsD.timing('conv.postProcess', new Date() - curDate);
    curDate = new Date();
  }
  if (tempDirs) {
    deleteFolderRecursive(tempDirs.temp);
    logger.debug('deleteFolderRecursive (id=%s)', dataConvert.key);
    if(clientStatsD) {
      clientStatsD.timing('conv.deleteFolderRecursive', new Date() - curDate);
      curDate = new Date();
    }
  }
  if(clientStatsD) {
    clientStatsD.timing('conv.allconvert', new Date() - startDate);
  }
  return resData;
}

function receiveTask(data) {
  return co(function* () {
    var res = null;
    var task = null;
    try {
      task = new commonDefines.TaskQueueData(JSON.parse(data));
      if (task) {
        res = yield* ExecuteTask(task);
      }
    } catch (err) {
      logger.error(err);
    } finally {
      try {
        if (!res && task) {
          var cmd = task.getCmd();
          cmd.setStatusInfo(constants.CONVERT);
          res = new commonDefines.TaskQueueData();
          res.setCmd(cmd);
        }
        if (res) {
          yield queue.addResponse(res);
        }
      } catch (err) {
        logger.error(err);
      }
    }
  });
}
function simulateErrorResponse(data){
  let task = new commonDefines.TaskQueueData(JSON.parse(data));
  let cmd = task.getCmd();
  cmd.setStatusInfo(constants.CONVERT);
  let res = new commonDefines.TaskQueueData();
  res.setCmd(cmd);
  return res;
}
function run() {
  queue = new queueService(simulateErrorResponse);
  queue.on('task', receiveTask);
  queue.init(true, true, true, false, false, false, function(err) {
    if (null != err) {
      logger.error('createTaskQueue error :\r\n%s', err.stack);
    }
  });
}
exports.run = run;
