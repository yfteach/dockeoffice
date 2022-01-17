/*
 * Copyright (C) Ascensio System SIA 2012-2019. All rights reserved
 *
 * https://www.onlyoffice.com/ 
 *
 * Version: 5.3.4 (build:3)
 */


'use strict';

var multiparty = require('multiparty');
var co = require('co');
var jwt = require('jsonwebtoken');
var taskResult = require('./taskresult');
var docsCoServer = require('./DocsCoServer');
var utils = require('./../../Common/sources/utils');
var constants = require('./../../Common/sources/constants');
var storageBase = require('./../../Common/sources/storage-base');
var formatChecker = require('./../../Common/sources/formatchecker');
var logger = require('./../../Common/sources/logger');
const commonDefines = require('./../../Common/sources/commondefines');

var config = require('config');
var configServer = config.get('services.CoAuthoring.server');
var configUtils = config.get('services.CoAuthoring.utils');

var cfgImageSize = configServer.get('limits_image_size');
var cfgTypesUpload = configUtils.get('limits_image_types_upload');
var cfgTokenEnableBrowser = config.get('services.CoAuthoring.token.enable.browser');

const PATTERN_ENCRYPTED = 'ENCRYPTED;';

exports.uploadTempFile = function(req, res) {
  return co(function* () {
    var docId = 'uploadTempFile';
    try {
      let params;
      let authRes = docsCoServer.getRequestParams(docId, req, true);
      if(authRes.code === constants.NO_ERROR){
        params = authRes.params;
      } else {
        utils.fillResponse(req, res, undefined, authRes.code, false);
        return;
      }
      docId = params.key;
      logger.debug('Start uploadTempFile: docId = %s', docId);
      if (docId && req.body && Buffer.isBuffer(req.body)) {
        var task = yield* taskResult.addRandomKeyTask(docId);
        var strPath = task.key + '/' + docId + '.tmp';
        yield storageBase.putObject(strPath, req.body, req.body.length);
        var url = yield storageBase.getSignedUrl(utils.getBaseUrlByRequest(req), strPath,
                                                 commonDefines.c_oAscUrlTypes.Temporary);
        utils.fillResponse(req, res, url, constants.NO_ERROR, false);
      } else {
        utils.fillResponse(req, res, undefined, constants.UNKNOWN, false);
      }
      logger.debug('End uploadTempFile: docId = %s', docId);
    }
    catch (e) {
      logger.error('Error uploadTempFile: docId = %s\r\n%s', docId, e.stack);
      utils.fillResponse(req, res, undefined, constants.UNKNOWN, false);
    }
  });
};
function checkJwtUpload(docId, errorName, token){
  var res = {err: true, docId: null, userid: null, encrypted: null};
  var checkJwtRes = docsCoServer.checkJwt(docId, token, commonDefines.c_oAscSecretType.Session);
  if (checkJwtRes.decoded) {
    var doc = checkJwtRes.decoded.document;
    var edit = checkJwtRes.decoded.editorConfig;
    if (!edit.ds_view && !edit.ds_isCloseCoAuthoring) {
      res.err = false;
      res.docId = doc.key;
      res.encrypted = doc.ds_encrypted;
      if (edit.user) {
        res.userid = edit.user.id;
      }
    } else {
      logger.warn('Error %s jwt: docId = %s\r\n%s', errorName, docId, 'access deny');
    }
  } else {
    logger.warn('Error %s jwt: docId = %s\r\n%s', errorName, docId, checkJwtRes.description);
  }
  return res;
}
exports.uploadImageFileOld = function(req, res) {
  var docId = req.params.docid;
  logger.debug('Start uploadImageFileOld: docId = %s', docId);
  var userid = req.params.userid;
  if (cfgTokenEnableBrowser) {
    var checkJwtRes = checkJwtUpload(docId, 'uploadImageFileOld', req.query['token']);
    if(!checkJwtRes.err){
      docId = checkJwtRes.docId || docId;
      userid = checkJwtRes.userid || userid;
    } else {
      res.sendStatus(403);
      return;
    }
  }
  var index = parseInt(req.params.index);
  var listImages = [];
  if (docId && index) {
    var isError = false;
    var form = new multiparty.Form();
    form.on('error', function(err) {
      logger.error('Error parsing form: docId = %s\r\n%s', docId, err.toString());
      res.sendStatus(400);
    });
    form.on('part', function(part) {
      if (!part.filename) {
        part.resume();
      }
      if (part.filename) {
        if (part.byteCount > cfgImageSize) {
          isError = true;
        }
        if (isError) {
          part.resume();
        } else {
          var strImageName = utils.crc32(userid).toString(16) + '_image' + (parseInt(index) + listImages.length);
          var strPath = docId + '/media/' + strImageName + '.jpg';
          listImages.push(strPath);
          utils.stream2Buffer(part).then(function(buffer) {
            return storageBase.putObject(strPath, buffer, buffer.length);
          }).then(function() {
            part.resume();
          }).catch(function(err) {
            logger.error('Upload putObject: docId = %s\r\n%s', docId, err.stack);
            isError = true;
            part.resume();
          });
        }
      }
      part.on('error', function(err) {
        logger.error('Error parsing form part: docId = %s\r\n%s', docId, err.toString());
      });
    });
    form.on('close', function() {
      if (isError) {
        res.sendStatus(400);
      } else {
        storageBase.getSignedUrlsByArray(utils.getBaseUrlByRequest(req), listImages, docId,
                                         commonDefines.c_oAscUrlTypes.Session).then(function(urls) {
          var outputData = {'type': 0, 'error': constants.NO_ERROR, 'urls': urls, 'input': req.query};
            var output = '<html><head><script type="text/javascript">function load(){ parent.postMessage("';
            output += JSON.stringify(outputData).replace(/"/g, '\\"');
            output += '", "*"); }</script></head><body onload="load()"></body></html>';
            res.setHeader('Content-Type', 'text/html');
            res.send(output);
            logger.debug('End uploadImageFileOld: docId = %s %s', docId, output);
          }
        ).catch(function(err) {
            res.sendStatus(400);
            logger.error('upload getSignedUrlsByArray: docId = %s\r\n%s', docId, err.stack);
          });
      }
    });
    form.parse(req);
  } else {
    logger.debug('Error params uploadImageFileOld: docId = %s', docId);
    res.sendStatus(400);
  }
};
exports.uploadImageFile = function(req, res) {
  return co(function* () {
    var isError = true;
    var docId = 'null';
    let output = {};
    let isValidJwt = true;
    try {
      docId = req.params.docid;
      var userid = req.params.userid;
      let encrypted = false;
      logger.debug('Start uploadImageFile: docId = %s', docId);

      if (cfgTokenEnableBrowser) {
        var checkJwtRes = checkJwtUpload(docId, 'uploadImageFile', req.query['token']);
        if (!checkJwtRes.err) {
          docId = checkJwtRes.docId || docId;
          userid = checkJwtRes.userid || userid;
          encrypted = checkJwtRes.encrypted;
        } else {
          isValidJwt = false;
        }
      }

      var index = parseInt(req.params.index);
      if (isValidJwt && docId && req.body && Buffer.isBuffer(req.body)) {
        let buffer = req.body;
        if (buffer.length <= cfgImageSize) {
          var format = formatChecker.getImageFormat(buffer, undefined);
          var formatStr = formatChecker.getStringFromFormat(format);
          var supportedFormats = cfgTypesUpload || 'jpg';
          let formatLimit = formatStr && -1 !== supportedFormats.indexOf(formatStr);
          if (!formatLimit && encrypted && PATTERN_ENCRYPTED == buffer.toString('utf8', 0, PATTERN_ENCRYPTED.length)) {
            formatLimit = true;
            formatStr = buffer.toString('utf8', PATTERN_ENCRYPTED.length, buffer.indexOf(';', PATTERN_ENCRYPTED.length));
          }
          if (formatLimit) {
            var strImageName = utils.crc32(userid).toString(16) + '_image' + index;
            var strPathRel = 'media/' + strImageName + '.' + formatStr;
            var strPath = docId + '/' + strPathRel;
            yield storageBase.putObject(strPath, buffer, buffer.length);
            output[strPathRel] = yield storageBase.getSignedUrl(utils.getBaseUrlByRequest(req), strPath,
                                                                commonDefines.c_oAscUrlTypes.Session);
            isError = false;
          } else {
            logger.debug('uploadImageFile format is not supported: docId = %s', docId);
          }
        } else {
          logger.debug('uploadImageFile size limit exceeded: buffer.length = %d docId = %s', buffer.length, docId);
        }
      }
    } catch (e) {
      isError = true;
      logger.error('Error uploadImageFile: docId = %s\r\n%s', docId, e.stack);
    } finally {
      try {
        if (!isError) {
          res.setHeader('Content-Type', 'application/json');
          res.send(JSON.stringify(output));
        } else {
          res.sendStatus(isValidJwt ? 400 : 403);
        }
        logger.debug('End uploadImageFile: isError = %s docId = %s', isError, docId);
      } catch (e) {
        logger.error('Error uploadImageFile: docId = %s\r\n%s', docId, e.stack);
      }
    }
  });
};
