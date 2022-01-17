/*
 * Copyright (C) Ascensio System SIA 2012-2019. All rights reserved
 *
 * https://www.onlyoffice.com/ 
 *
 * Version: 5.3.4 (build:3)
 */


'use strict';

var statsD = require('node-statsd');
var configStatsD = require('config').get('statsd');

var cfgStatsDUseMetrics = configStatsD.get('useMetrics');
var cfgStatsDHost = configStatsD.get('host');
var cfgStatsDPort = configStatsD.get('port');
var cfgStatsDPrefix = configStatsD.get('prefix');

var clientStatsD = null;
if(cfgStatsDUseMetrics) {
  clientStatsD = new statsD({host: cfgStatsDHost, port:cfgStatsDPort, prefix: cfgStatsDPrefix});
}

exports.getClient = function() {
  return clientStatsD;
};
