/*
 * Copyright (C) Ascensio System SIA 2012-2019. All rights reserved
 *
 * https://www.onlyoffice.com/ 
 *
 * Version: 5.3.4 (build:3)
 */


'use strict';
var config = require('config');
var utils = require('./utils');
var logger = require('./logger');

var storage = require('./' + config.get('storage.name'));
function getStoragePath(strPath) {
  return strPath.replace(/\\/g, '/');
}
exports.getObject = function(strPath) {
  return storage.getObject(getStoragePath(strPath));
};
exports.putObject = function(strPath, buffer, contentLength) {
  return storage.putObject(getStoragePath(strPath), buffer, contentLength);
};
exports.uploadObject = function(strPath, filePath) {
  return storage.uploadObject(strPath, filePath);
};
exports.copyObject = function(sourceKey, destinationKey) {
  return storage.copyObject(sourceKey, destinationKey);
};
exports.copyPath = function(sourcePath, destinationPath) {
  return exports.listObjects(getStoragePath(sourcePath)).then(function(list) {
    return Promise.all(list.map(function(curValue) {
      return exports.copyObject(curValue, destinationPath + '/' + exports.getRelativePath(sourcePath, curValue));
    }));
  });
};
exports.listObjects = function(strPath) {
  return storage.listObjects(getStoragePath(strPath)).catch(function(e) {
    logger.error('storage.listObjects:\r\n%s', e.stack);
    return [];
  });
};
exports.deleteObject = function(strPath) {
  return storage.deleteObject(getStoragePath(strPath));
};
exports.deleteObjects = function(strPaths) {
  var StoragePaths = strPaths.map(function(curValue) {
    return getStoragePath(curValue);
  });
  return storage.deleteObjects(StoragePaths);
};
exports.deletePath = function(strPath) {
  return exports.listObjects(getStoragePath(strPath)).then(function(list) {
    return exports.deleteObjects(list);
  });
};
exports.getSignedUrl = function(baseUrl, strPath, urlType, optFilename, opt_type) {
  return storage.getSignedUrl(baseUrl, getStoragePath(strPath), urlType, optFilename, opt_type);
};
exports.getSignedUrls = function(baseUrl, strPath, urlType) {
  return exports.listObjects(getStoragePath(strPath)).then(function(list) {
    return Promise.all(list.map(function(curValue) {
      return exports.getSignedUrl(baseUrl, curValue, urlType);
    })).then(function(urls) {
      var outputMap = {};
      for (var i = 0; i < list.length && i < urls.length; ++i) {
        outputMap[exports.getRelativePath(strPath, list[i])] = urls[i];
      }
      return outputMap;
    });
  });
};
exports.getSignedUrlsArrayByArray = function(baseUrl, list, urlType, opt_type) {
  return Promise.all(list.map(function(curValue) {
    return exports.getSignedUrl(baseUrl, curValue, urlType, undefined, opt_type);
  }));
};
exports.getSignedUrlsByArray = function(baseUrl, list, optPath, urlType) {
  return exports.getSignedUrlsArrayByArray(baseUrl, list, urlType).then(function(urls) {
    var outputMap = {};
    for (var i = 0; i < list.length && i < urls.length; ++i) {
      if (optPath) {
        outputMap[exports.getRelativePath(optPath, list[i])] = urls[i];
      } else {
        outputMap[list[i]] = urls[i];
      }
    }
    return outputMap;
  });
};
exports.getRelativePath = function(strBase, strPath) {
  return strPath.substring(strBase.length + 1);
};
