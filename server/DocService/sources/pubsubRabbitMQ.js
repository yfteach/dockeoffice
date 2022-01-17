/*
 * Copyright (C) Ascensio System SIA 2012-2019. All rights reserved
 *
 * https://www.onlyoffice.com/ 
 *
 * Version: 5.3.4 (build:3)
 */


'use strict';
var config = require('config');
var events = require('events');
var util = require('util');
var co = require('co');
var constants = require('./../../Common/sources/constants');
const commonDefines = require('./../../Common/sources/commondefines');
var utils = require('./../../Common/sources/utils');
var rabbitMQCore = require('./../../Common/sources/rabbitMQCore');
var activeMQCore = require('./../../Common/sources/activeMQCore');
const logger = require('./../../Common/sources/logger');

const cfgQueueType = config.get('queue.type');
var cfgRabbitExchangePubSub = config.get('rabbitmq.exchangepubsub');
var cfgActiveTopicPubSub = constants.ACTIVEMQ_TOPIC_PREFIX + config.get('activemq.topicpubsub');

function initRabbit(pubsub, callback) {
  return co(function* () {
    var e = null;
    try {
      var conn = yield rabbitMQCore.connetPromise(true, function() {
        clear(pubsub);
        if (!pubsub.isClose) {
          init(pubsub, null);
        }
      });
      pubsub.connection = conn;
      pubsub.channelPublish = yield rabbitMQCore.createChannelPromise(conn);
      pubsub.exchangePublish = yield rabbitMQCore.assertExchangePromise(pubsub.channelPublish, cfgRabbitExchangePubSub,
        'fanout', {durable: true});

      pubsub.channelReceive = yield rabbitMQCore.createChannelPromise(conn);
      var queue = yield rabbitMQCore.assertQueuePromise(pubsub.channelReceive, '', {autoDelete: true, exclusive: true});
      pubsub.channelReceive.bindQueue(queue, cfgRabbitExchangePubSub, '');
      yield rabbitMQCore.consumePromise(pubsub.channelReceive, queue, function (message) {
        if(null != pubsub.channelReceive){
          if (message) {
            pubsub.emit('message', message.content.toString());
          }
          pubsub.channelReceive.ack(message);
        }
      }, {noAck: false});
      repeat(pubsub);
    } catch (err) {
      e = err;
    }
    if (callback) {
      callback(e);
    }
  });
}
function initActive(pubsub, callback) {
  return co(function*() {
    var e = null;
    try {
      var conn = yield activeMQCore.connetPromise(true, function() {
        clear(pubsub);
        if (!pubsub.isClose) {
          init(pubsub, null);
        }
      });
      pubsub.connection = conn;
      pubsub.channelPublish = yield activeMQCore.openSenderPromise(conn, cfgActiveTopicPubSub);

      let receiver = yield activeMQCore.openReceiverPromise(conn, cfgActiveTopicPubSub, false);
      receiver.add_credit(1);
      receiver.on("message", function(context) {
        if (context) {
          pubsub.emit('message', context.message.body);
        }

        context.delivery.accept();
        receiver.add_credit(1);
      });
      repeat(pubsub);
    } catch (err) {
      e = err;
    }
    if (callback) {
      callback(e);
    }
  });
}
function clear(pubsub) {
  pubsub.channelPublish = null;
  pubsub.exchangePublish = null;
  pubsub.channelReceive = null;
}
function repeat(pubsub) {
  for (var i = 0; i < pubsub.publishStore.length; ++i) {
    publish(pubsub, pubsub.publishStore[i]);
  }
  pubsub.publishStore.length = 0;
}
function publishRabbit(pubsub, data) {
  pubsub.channelPublish.publish(pubsub.exchangePublish, '', data);
}
function publishActive(pubsub, data) {
  pubsub.channelPublish.send({durable: true, body: data});
}
function closeRabbit(conn) {
  return rabbitMQCore.closePromise(conn);
}
function closeActive(conn) {
  return activeMQCore.closePromise(conn);
}

let init;
let publish;
let close;
if (commonDefines.c_oAscQueueType.rabbitmq === cfgQueueType) {
  init = initRabbit;
  publish = publishRabbit;
  close = closeRabbit;
} else {
  init = initActive;
  publish = publishActive;
  close = closeActive;
}

function PubsubRabbitMQ() {
  this.isClose = false;
  this.connection = null;
  this.channelPublish = null;
  this.exchangePublish = null;
  this.channelReceive = null;
  this.publishStore = [];
}
util.inherits(PubsubRabbitMQ, events.EventEmitter);
PubsubRabbitMQ.prototype.init = function (callback) {
  init(this, callback);
};
PubsubRabbitMQ.prototype.initPromise = function() {
  var t = this;
  return new Promise(function(resolve, reject) {
    init(t, function(err) {
      if (err) {
        reject(err);
      } else {
        resolve();
      }
    });
  });
};
PubsubRabbitMQ.prototype.publish = function (message) {
  var data = new Buffer(message);
  if (null != this.channelPublish) {
    publish(this, data);
  } else {
    this.publishStore.push(data);
  }
};
PubsubRabbitMQ.prototype.close = function() {
  this.isClose = true;
  return close(this.connection);
};

module.exports = PubsubRabbitMQ;
