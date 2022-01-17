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
var utils = require('./utils');
var constants = require('./constants');
var rabbitMQCore = require('./rabbitMQCore');
var activeMQCore = require('./activeMQCore');
const logger = require('./logger');
const commonDefines = require('./commondefines');

const cfgMaxRedeliveredCount = config.get('FileConverter.converter.maxRedeliveredCount');
const cfgQueueType = config.get('queue.type');
var cfgVisibilityTimeout = config.get('queue.visibilityTimeout');
var cfgQueueRetentionPeriod = config.get('queue.retentionPeriod');
var cfgRabbitQueueConvertTask = config.get('rabbitmq.queueconverttask');
var cfgRabbitQueueConvertResponse = config.get('rabbitmq.queueconvertresponse');
var cfgRabbitExchangeConvertDead = config.get('rabbitmq.exchangeconvertdead');
var cfgRabbitQueueConvertDead = config.get('rabbitmq.queueconvertdead');
var cfgRabbitQueueDelayed = config.get('rabbitmq.queuedelayed');
var cfgActiveQueueConvertTask = constants.ACTIVEMQ_QUEUE_PREFIX + config.get('activemq.queueconverttask');
var cfgActiveQueueConvertResponse = constants.ACTIVEMQ_QUEUE_PREFIX + config.get('activemq.queueconvertresponse');
var cfgActiveQueueConvertDead = constants.ACTIVEMQ_QUEUE_PREFIX + config.get('activemq.queueconvertdead');
var cfgActiveQueueDelayed = constants.ACTIVEMQ_QUEUE_PREFIX + config.get('activemq.queuedelayed');

function initRabbit(taskqueue, isAddTask, isAddResponse, isAddTaskReceive, isAddResponseReceive, isEmitDead, isAddDelayed, callback) {
  return co(function* () {
    var e = null;
    try {
      var conn = yield rabbitMQCore.connetPromise(true, function() {
        clear(taskqueue);
        if (!taskqueue.isClose) {
          init(taskqueue, isAddTask, isAddResponse, isAddTaskReceive, isAddResponseReceive, isEmitDead, isAddDelayed, null);
        }
      });
      taskqueue.connection = conn;
      var bAssertTaskQueue = false;
      var optionsTaskQueue = {
        durable: true,
        maxPriority: constants.QUEUE_PRIORITY_VERY_HIGH,
        messageTtl: cfgQueueRetentionPeriod * 1000,
        deadLetterExchange: cfgRabbitExchangeConvertDead
      };
      if (isAddTask) {
        taskqueue.channelConvertTask = yield rabbitMQCore.createConfirmChannelPromise(conn);
        yield rabbitMQCore.assertQueuePromise(taskqueue.channelConvertTask, cfgRabbitQueueConvertTask,
          optionsTaskQueue);
        bAssertTaskQueue = true;
      }
      var bAssertResponseQueue = false;
      var optionsResponseQueue = {durable: true};
      if (isAddResponse) {
        taskqueue.channelConvertResponse = yield rabbitMQCore.createConfirmChannelPromise(conn);
        yield rabbitMQCore.assertQueuePromise(taskqueue.channelConvertResponse, cfgRabbitQueueConvertResponse,
          optionsResponseQueue);
        bAssertResponseQueue = true;
      }
      var optionsReceive = {noAck: false};
      if (isAddTaskReceive) {
        taskqueue.channelConvertTaskReceive = yield rabbitMQCore.createChannelPromise(conn);
        taskqueue.channelConvertTaskReceive.prefetch(1);
        if (!bAssertTaskQueue) {
          yield rabbitMQCore.assertQueuePromise(taskqueue.channelConvertTaskReceive, cfgRabbitQueueConvertTask,
            optionsTaskQueue);
        }
        yield rabbitMQCore.consumePromise(taskqueue.channelConvertTaskReceive, cfgRabbitQueueConvertTask,
          function (message) {
            co(function* () {
              let redelivered = yield* pushBackRedeliveredRabbit(taskqueue, message);
              if (!redelivered) {
                if (message) {
                  taskqueue.emit('task', message.content.toString());
                }
                taskqueue.channelConvertTaskReceive.ack(message);
              }
            });
          }, optionsReceive);
      }
      if (isAddResponseReceive) {
        taskqueue.channelConvertResponseReceive = yield rabbitMQCore.createChannelPromise(conn);
        if (!bAssertResponseQueue) {
          yield rabbitMQCore.assertQueuePromise(taskqueue.channelConvertResponseReceive, cfgRabbitQueueConvertResponse,
            optionsResponseQueue);
        }
        yield rabbitMQCore.consumePromise(taskqueue.channelConvertResponseReceive, cfgRabbitQueueConvertResponse,
          function (message) {
            if (message) {
              taskqueue.emit('response', message.content.toString());
            }
            taskqueue.channelConvertResponseReceive.ack(message);
          }, optionsReceive);
      }
      if (isAddDelayed) {
        let optionsDelayedQueue = {
          durable: true,
          deadLetterExchange: cfgRabbitExchangeConvertDead
        };
        taskqueue.channelDelayed = yield rabbitMQCore.createConfirmChannelPromise(conn);
        yield rabbitMQCore.assertQueuePromise(taskqueue.channelDelayed, cfgRabbitQueueDelayed, optionsDelayedQueue);
      }
      if (isEmitDead) {
        taskqueue.channelConvertDead = yield rabbitMQCore.createChannelPromise(conn);
        yield rabbitMQCore.assertExchangePromise(taskqueue.channelConvertDead, cfgRabbitExchangeConvertDead, 'fanout',
                                                 {durable: true});
        var queue = yield rabbitMQCore.assertQueuePromise(taskqueue.channelConvertDead, cfgRabbitQueueConvertDead,
                                                          {durable: true});

        taskqueue.channelConvertDead.bindQueue(queue, cfgRabbitExchangeConvertDead, '');
        yield rabbitMQCore.consumePromise(taskqueue.channelConvertDead, queue, function(message) {
          if (null != taskqueue.channelConvertDead) {
            if (message) {
              taskqueue.emit('dead', message.content.toString());
            }
            taskqueue.channelConvertDead.ack(message);
          }
        }, {noAck: false});
      }
      repeat(taskqueue);
    } catch (err) {
      e = err;
    }
    if (callback) {
      callback(e);
    }
  });
}
function initActive(taskqueue, isAddTask, isAddResponse, isAddTaskReceive, isAddResponseReceive, isEmitDead, isAddDelayed, callback) {
  return co(function*() {
    var e = null;
    try {
      var conn = yield activeMQCore.connetPromise(true, function() {
        clear(taskqueue);
        if (!taskqueue.isClose) {
          init(taskqueue, isAddTask, isAddResponse, isAddTaskReceive, isAddResponseReceive, isEmitDead, isAddDelayed, null);
        }
      });
      taskqueue.connection = conn;
      if (isAddTask) {
        taskqueue.channelConvertTask = yield activeMQCore.openSenderPromise(conn, cfgActiveQueueConvertTask);
      }
      if (isAddResponse) {
        taskqueue.channelConvertResponse = yield activeMQCore.openSenderPromise(conn, cfgActiveQueueConvertResponse);
      }
      if (isAddTaskReceive) {
        let receiver = yield activeMQCore.openReceiverPromise(conn, cfgActiveQueueConvertTask, false);
        receiver.add_credit(1);
        receiver.on("message", function(context) {
          co(function*() {
            let redelivered = yield* pushBackRedeliveredActive(taskqueue, context);
            if (!redelivered) {
              if (context) {
                taskqueue.emit('task', context.message.body);
              }
              context.delivery.accept();
              receiver.add_credit(1);
            }
          });
        });
        taskqueue.channelConvertTaskReceive = receiver;
      }
      if (isAddResponseReceive) {
        let receiver = yield activeMQCore.openReceiverPromise(conn, cfgActiveQueueConvertResponse, false);
        receiver.add_credit(1);
        receiver.on("message", function(context) {
          if (context) {
            taskqueue.emit('response', context.message.body);
          }
          context.delivery.accept();
          receiver.add_credit(1);
        });
        taskqueue.channelConvertResponseReceive = receiver;
      }
      if (isAddDelayed) {
        taskqueue.channelDelayed = yield activeMQCore.openSenderPromise(conn, cfgActiveQueueDelayed);
      }
      if (isEmitDead) {
        let receiver = yield activeMQCore.openReceiverPromise(conn, cfgActiveQueueConvertDead, false);
        receiver.add_credit(1);
        receiver.on("message", function(context) {
          if (context) {
            taskqueue.emit('dead', context.message.body);
          }
          context.delivery.accept();
          receiver.add_credit(1);
        });
        taskqueue.channelConvertDead = receiver;
      }
      repeat(taskqueue);
    } catch (err) {
      e = err;
    }
    if (callback) {
      callback(e);
    }
  });
}
function clear(taskqueue) {
  taskqueue.channelConvertTask = null;
  taskqueue.channelConvertTaskReceive = null;
  taskqueue.channelConvertDead = null;
  taskqueue.channelConvertResponse = null;
  taskqueue.channelConvertResponseReceive = null;
  taskqueue.channelDelayed = null;
}
function* pushBackRedeliveredRabbit(taskqueue, message) {
  if (message.fields.redelivered) {
    try {
      logger.warn('checkRedelivered redelivered data=%j', message);
      taskqueue.channelConvertTaskReceive.ack(message);

      let data = message.content.toString();
      let redeliveredCount = message.properties.headers['x-redelivered-count'];
      if (!redeliveredCount || redeliveredCount < cfgMaxRedeliveredCount) {
        message.properties.headers['x-redelivered-count'] = redeliveredCount ? redeliveredCount + 1 : 1;
        yield addTaskString(taskqueue, data, message.properties.priority, undefined, message.properties.headers);
      } else if (taskqueue.simulateErrorResponse) {
        yield taskqueue.addResponse(taskqueue.simulateErrorResponse(data));
      }
    } catch (err) {
      logger.error('checkRedelivered error: %s', err.stack);
    }
    return true;
  }
  return false;
}
function* pushBackRedeliveredActive(taskqueue, context) {
  if (undefined !== context.message.delivery_count) {
    logger.warn('checkRedelivered redelivered data=%j', context.message);
    if (context.message.delivery_count > cfgMaxRedeliveredCount) {
      context.delivery.accept();
      taskqueue.channelConvertTaskReceive.add_credit(1);

      if (taskqueue.simulateErrorResponse) {
        yield taskqueue.addResponse(taskqueue.simulateErrorResponse(context.message.body));
      }
      return true;
    }
  }
  return false;
}
function repeat(taskqueue) {
  for (var i = 0; i < taskqueue.addTaskStore.length; ++i) {
    var elem = taskqueue.addTaskStore[i];
    addTask(taskqueue, elem.task, elem.priority, function () {}, elem.expiration, elem.headers);
  }
  taskqueue.addTaskStore.length = 0;
  for (var i = 0; i < taskqueue.addDelayedStore.length; ++i) {
    var elem = taskqueue.addDelayedStore[i];
    addDelayed(taskqueue, elem.task, elem.ttl, function () {});
  }
  taskqueue.addDelayedStore.length = 0;
}
function addTaskRabbit(taskqueue, content, priority, callback, opt_expiration, opt_headers) {
  var options = {persistent: true, priority: priority};
  if (undefined !== opt_expiration) {
    options.expiration = opt_expiration.toString();
  }
  if (undefined !== opt_headers) {
    options.headers = opt_headers;
  }
  taskqueue.channelConvertTask.sendToQueue(cfgRabbitQueueConvertTask, content, options, callback);
}
function addTaskActive(taskqueue, content, priority, callback, opt_expiration, opt_headers) {
  var msg = {durable: true, priority: priority, body: content, ttl: cfgQueueRetentionPeriod * 1000};
  if (undefined !== opt_expiration) {
    msg.ttl = opt_expiration;
  }
  taskqueue.channelConvertTask.send(msg);
  callback();
}
function addTaskString(taskqueue, task, priority, opt_expiration, opt_headers) {
  return new Promise(function (resolve, reject) {
    var content = new Buffer(task);
    if (null != taskqueue.channelConvertTask) {
      addTask(taskqueue, content, priority, function (err, ok) {
        if (null != err) {
          reject(err);
        } else {
          resolve();
        }
      }, opt_expiration, opt_headers);
    } else {
      taskqueue.addTaskStore.push({task: content, priority: priority, expiration: opt_expiration, headers: opt_headers});
      resolve();
    }
  });
}
function addResponseRabbit(taskqueue, content, callback) {
  var options = {persistent: true};
  taskqueue.channelConvertResponse.sendToQueue(cfgRabbitQueueConvertResponse, content, options, callback);
}
function addResponseActive(taskqueue, content, callback) {
  var msg = {durable: true, body: content};
  taskqueue.channelConvertResponse.send(msg);
  callback();
}
function closeRabbit(conn) {
  return rabbitMQCore.closePromise(conn);
}
function closeActive(conn) {
  return activeMQCore.closePromise(conn);
}
function addDelayedRabbit(taskqueue, content, ttl, callback) {
  var options = {persistent: true, expiration: ttl.toString()};
  taskqueue.channelDelayed.sendToQueue(cfgRabbitQueueDelayed, content, options, callback);
}
function addDelayedActive(taskqueue, content, ttl, callback) {
  var msg = {durable: true, body: content, ttl: ttl};
  taskqueue.channelDelayed.send(msg);
  callback();
}

let init;
let addTask;
let addResponse;
let close;
let addDelayed;
if (commonDefines.c_oAscQueueType.rabbitmq === cfgQueueType) {
  init = initRabbit;
  addTask = addTaskRabbit;
  addResponse = addResponseRabbit;
  close = closeRabbit;
  addDelayed = addDelayedRabbit;
} else {
  init = initActive;
  addTask = addTaskActive;
  addResponse = addResponseActive;
  close = closeActive;
  addDelayed = addDelayedActive;
}

function TaskQueueRabbitMQ(simulateErrorResponse) {
  this.isClose = false;
  this.connection = null;
  this.channelConvertTask = null;
  this.channelConvertTaskReceive = null;
  this.channelConvertDead = null;
  this.channelConvertResponse = null;
  this.channelConvertResponseReceive = null;
  this.channelDelayed = null;
  this.addTaskStore = [];
  this.addDelayedStore = [];
  this.simulateErrorResponse = simulateErrorResponse;
}
util.inherits(TaskQueueRabbitMQ, events.EventEmitter);
TaskQueueRabbitMQ.prototype.init = function (isAddTask, isAddResponse, isAddTaskReceive, isAddResponseReceive, isEmitDead, isAddDelayed, callback) {
  init(this, isAddTask, isAddResponse, isAddTaskReceive, isAddResponseReceive, isEmitDead, isAddDelayed, callback);
};
TaskQueueRabbitMQ.prototype.initPromise = function(isAddTask, isAddResponse, isAddTaskReceive, isAddResponseReceive, isEmitDead, isAddDelayed) {
  var t = this;
  return new Promise(function(resolve, reject) {
    init(t, isAddTask, isAddResponse, isAddTaskReceive, isAddResponseReceive, isEmitDead, isAddDelayed, function(err) {
      if (err) {
        reject(err);
      } else {
        resolve();
      }
    });
  });
};
TaskQueueRabbitMQ.prototype.addTask = function (task, priority, opt_expiration, opt_headers) {
  task.setVisibilityTimeout(cfgVisibilityTimeout);
  return addTaskString(this, JSON.stringify(task), priority, opt_expiration);
};
TaskQueueRabbitMQ.prototype.addResponse = function (task) {
  var t = this;
  return new Promise(function (resolve, reject) {
    var content = new Buffer(JSON.stringify(task));
    if (null != t.channelConvertResponse) {
      addResponse(t, content, function (err, ok) {
        if (null != err) {
          reject(err);
        } else {
          resolve();
        }
      });
    } else {
      resolve();
    }
  });
};
TaskQueueRabbitMQ.prototype.addDelayed = function (task, ttl) {
  var t = this;
  return new Promise(function (resolve, reject) {
    var content = new Buffer(JSON.stringify(task));
    if (null != t.channelDelayed) {
      addDelayed(t, content, ttl, function (err, ok) {
        if (null != err) {
          reject(err);
        } else {
          resolve();
        }
      });
    } else {
      t.addDelayedStore.push({task: content, ttl: ttl});
      resolve();
    }
  });
};
TaskQueueRabbitMQ.prototype.close = function () {
  this.isClose = true;
  return close(this.connection);
};

module.exports = TaskQueueRabbitMQ;
