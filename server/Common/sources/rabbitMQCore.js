/*
 * Copyright (C) Ascensio System SIA 2012-2019. All rights reserved
 *
 * https://www.onlyoffice.com/ 
 *
 * Version: 5.3.4 (build:3)
 */


'use strict';
var config = require('config');
var amqp = require('amqplib/callback_api');
var logger = require('./logger');

var cfgRabbitUrl = config.get('rabbitmq.url');
var cfgRabbitSocketOptions = config.get('rabbitmq.socketOptions');

var RECONNECT_TIMEOUT = 1000;

function connetPromise(reconnectOnConnectionError, closeCallback) {
  return new Promise(function(resolve, reject) {
    function startConnect() {
      amqp.connect(cfgRabbitUrl, cfgRabbitSocketOptions, function(err, conn) {
        if (null != err) {
          logger.error('[AMQP] %s', err.stack);
          if (reconnectOnConnectionError) {
            setTimeout(startConnect, RECONNECT_TIMEOUT);
          } else {
            reject(err);
          }
        } else {
          conn.on('error', function(err) {
            logger.error('[AMQP] conn error', err.stack);
          });
          var closeEventCallback = function() {
            conn.removeListener('close', closeEventCallback);
            logger.debug('[AMQP] conn close');
            closeCallback();
          };
          conn.on('close', closeEventCallback);
          logger.debug('[AMQP] connected');
          resolve(conn);
        }
      });
    }
    startConnect();
  });
}
function createChannelPromise(conn) {
  return new Promise(function(resolve, reject) {
    conn.createChannel(function(err, channel) {
      if (null != err) {
        reject(err);
      } else {
        resolve(channel);
      }
    });
  });
}
function createConfirmChannelPromise(conn) {
  return new Promise(function(resolve, reject) {
    conn.createConfirmChannel(function(err, channel) {
      if (null != err) {
        reject(err);
      } else {
        resolve(channel);
      }
    });
  });
}
function assertExchangePromise(channel, exchange, type, options) {
  return new Promise(function(resolve, reject) {
    channel.assertExchange(exchange, type, options, function(err, ok) {
      if (null != err) {
        reject(err);
      } else {
        resolve(ok.exchange);
      }
    });
  });
}
function assertQueuePromise(channel, queue, options) {
  return new Promise(function(resolve, reject) {
    channel.assertQueue(queue, options, function(err, ok) {
      if (null != err) {
        reject(err);
      } else {
        resolve(ok.queue);
      }
    });
  });
}
function consumePromise(channel, queue, messageCallback, options) {
  return new Promise(function(resolve, reject) {
    channel.consume(queue, messageCallback, options, function(err, ok) {
      if (null != err) {
        reject(err);
      } else {
        resolve(ok);
      }
    });
  });
}
function closePromise(conn) {
  return new Promise(function(resolve, reject) {
    conn.close(function(err) {
      if (err) {
        reject(err);
      } else {
        resolve();
      }
    });
  });
}

module.exports.connetPromise = connetPromise;
module.exports.createChannelPromise = createChannelPromise;
module.exports.createConfirmChannelPromise = createConfirmChannelPromise;
module.exports.assertExchangePromise = assertExchangePromise;
module.exports.assertQueuePromise = assertQueuePromise;
module.exports.consumePromise = consumePromise;
module.exports.closePromise = closePromise;
