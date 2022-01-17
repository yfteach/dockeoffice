/*
 * Copyright (C) Ascensio System SIA 2012-2019. All rights reserved
 *
 * https://www.onlyoffice.com/ 
 *
 * Version: 5.3.4 (build:3)
 */

'use strict';
var config = require('config');
var container = require('rhea');
var logger = require('./logger');

const cfgRabbitSocketOptions = config.get('activemq.connectOptions');

var RECONNECT_TIMEOUT = 1000;

function connetPromise(reconnectOnConnectionError, closeCallback) {
  return new Promise(function(resolve, reject) {
    function startConnect() {
      let conn = container.create_container().connect(cfgRabbitSocketOptions);
      let isConnected = false;
      conn.on('connection_open', function(context) {
        logger.debug('[AMQP] connected');
        isConnected = true;
        resolve(conn);
      });
      conn.on('connection_error', function(context) {
        logger.debug('[AMQP] connection_error %s', context.error);
      });
      conn.on('connection_close', function() {
        logger.debug('[AMQP] conn close');
      });
      conn.on('disconnected', function(context) {
        logger.error('[AMQP] disconnected %s', context.error && context.error.stack);
        if (isConnected) {
          closeCallback();
        } else {
          if (reconnectOnConnectionError) {
            setTimeout(startConnect, RECONNECT_TIMEOUT);
          } else {
            reject(context.error);
          }
        }
      });
    }

    startConnect();
  });
}
function openSenderPromise(conn, name) {
  return new Promise(function(resolve, reject) {
    let options = {target: name};
    resolve(conn.open_sender(options));
  });
}
function openReceiverPromise(conn, name, autoaccept) {
  return new Promise(function(resolve, reject) {
    let options = {source: name};
    if (!autoaccept) {
      options.credit_window = 0;
      options.autoaccept = false;
    }
    resolve(conn.open_receiver(options));
  });
}
function closePromise(conn) {
  return new Promise(function(resolve, reject) {
    conn.close();
    resolve();
  });
}

module.exports.connetPromise = connetPromise;
module.exports.openSenderPromise = openSenderPromise;
module.exports.openReceiverPromise = openReceiverPromise;
module.exports.closePromise = closePromise;
