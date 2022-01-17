/*
 * Copyright (C) Ascensio System SIA 2012-2019. All rights reserved
 *
 * https://www.onlyoffice.com/ 
 *
 * Version: 5.3.4 (build:3)
 */


'use strict';

const crypto = require('crypto');
const fs = require('fs');
const config = require('config');
const configL = config.get('license');
const constants = require('./constants');
const logger = require('./logger');
const utils = require('./utils');
const pubsubRedis = require('./../../DocService/sources/pubsubRedis');
const redisClient = pubsubRedis.getClientRedis();

const buildDate = '2019-07-12';
const oBuildDate = new Date(buildDate);
const oPackageType = constants.PACKAGE_TYPE_OS;

const cfgRedisPrefix = config.get('services.CoAuthoring.redis.prefix');
const redisKeyLicense = cfgRedisPrefix + ((constants.PACKAGE_TYPE_OS === oPackageType) ? constants.REDIS_KEY_LICENSE :
	constants.REDIS_KEY_LICENSE_T);

exports.readLicense = function*() {
	const c_LR = constants.LICENSE_RESULT;
	const c_LM = constants.LICENSE_MODE;
	const resMax = {count: 999999, type: c_LR.Success, mode: c_LM.None, connections: 999999999, users: 999999999};
	const res = {
		count: 1,
		type: c_LR.Error,
		light: false,
		packageType: oPackageType,
		mode: c_LM.None,
		branding: false,
		connections: constants.LICENSE_CONNECTIONS,
		customization: false,
		usersCount: 0,
		usersExpire: constants.LICENSE_EXPIRE_USERS_ONE_DAY,
		hasLicense: false,
		plugins: false,
		buildDate: oBuildDate,
		endDate: null
	};
	let checkFile = false;
	try {
		const oFile = fs.readFileSync(configL.get('license_file')).toString();
		res.hasLicense = checkFile = true;
		const oLicense = JSON.parse(oFile);
		const sign = oLicense['signature'];
		delete oLicense['signature'];

		const verify = crypto.createVerify('RSA-SHA1');
		verify.update(JSON.stringify(oLicense));
		const publicKey = '-----BEGIN PUBLIC KEY-----\nMIGfMA0GCSqGSIb3DQEBAQUAA4GNADCBiQKBgQDRhGF7X4A0ZVlEg594WmODVVUI\niiPQs04aLmvfg8SborHss5gQXu0aIdUT6nb5rTh5hD2yfpF2WIW6M8z0WxRhwicg\nXwi80H1aLPf6lEPPLvN29EhQNjBpkFkAJUbS8uuhJEeKw0cE49g80eBBF4BCqSL6\nPFQbP9/rByxdxEoAIQIDAQAB\n-----END PUBLIC KEY-----\n';
		if (verify.verify(publicKey, sign, 'hex')) {
			const endDate = new Date(oLicense['end_date']);
			res.endDate = endDate;
			const isTrial = (true === oLicense['trial'] || 'true' === oLicense['trial']); // Someone who likes to put json string instead of bool
			res.mode = isTrial ? c_LM.Trial : getLicenseMode(oLicense['mode']);
			const checkDate = c_LM.Trial === res.mode ? new Date() : oBuildDate;
			if (endDate >= checkDate && 2 <= oLicense['version']) {
				res.connections = Math.max(res.count, oLicense['process'] >> 0) * 75;
				res.count = resMax.count;
				res.type = c_LR.Success;
			} else {
				res.type = isTrial ? c_LR.ExpiredTrial : c_LR.Expired;
			}

			res.light = (true === oLicense['light'] || 'true' === oLicense['light']); // Someone who likes to put json string instead of bool
			res.branding = (true === oLicense['branding'] || 'true' === oLicense['branding']); // Someone who likes to put json string instead of bool
			res.customization = (!oLicense.hasOwnProperty('customization') || !!oLicense['customization']); // Check exist property for old licenses
			res.plugins = true === oLicense['plugins'];
			if (oLicense.hasOwnProperty('connections')) {
				res.connections = oLicense['connections'] >> 0;
			}
			if (oLicense.hasOwnProperty('users_count')) {
				res.usersCount = oLicense['users_count'] >> 0;
			}
			if (oLicense.hasOwnProperty('users_expire')) {
				res.usersExpire = Math.max(constants.LICENSE_EXPIRE_USERS_ONE_DAY, (oLicense['users_expire'] >> 0) *
					constants.LICENSE_EXPIRE_USERS_ONE_DAY);
			}
		} else {
			throw 'verify';
		}
	} catch (e) {
		res.count = 1;
		res.type = c_LR.Error;

		if (checkFile) {
			res.type = c_LR.ExpiredTrial;
		} else {
			if (constants.PACKAGE_TYPE_OS === oPackageType) {
				if (yield* _getFileState(res)) {
					res.type = c_LR.ExpiredTrial;
				}
			} else {
				res.type = (yield* _getFileState(res)) ? c_LR.Success : c_LR.ExpiredTrial;
				if (res.type === c_LR.Success) {
					res.mode = c_LM.Trial;
					res.count = resMax.count;
					res.customization = constants.PACKAGE_TYPE_D === oPackageType;
					return res;
				}
			}
		}
	}
	if (res.type === c_LR.Expired || res.type === c_LR.ExpiredTrial) {
		res.count = 1;
		logger.error('License: License Expired!!!');
	}

	if (checkFile) {
		yield* _updateFileState(true);
	}

	return res;
};
exports.packageType = oPackageType;

function getLicenseMode(mode) {
	const c_LM = constants.LICENSE_MODE;
	return 'developer' === mode ? c_LM.Developer : ('trial' === mode ? c_LM.Trial : c_LM.None);
}

function* _getFileState(res) {
	const val = yield utils.promiseRedis(redisClient, redisClient.hget, redisKeyLicense, redisKeyLicense);
	if (constants.PACKAGE_TYPE_OS === oPackageType) {
		return val;
	}

	if (null === val) {
		yield* _updateFileState(false);
		return true;
	}

	var endDate = new Date(val);
	endDate.setMonth(endDate.getMonth() + 1);
	res.endDate = endDate;
	return (0 >= (new Date() - endDate));
}
function* _updateFileState(state) {
	const val = constants.PACKAGE_TYPE_OS === oPackageType ? redisKeyLicense : (state ? new Date(1) : new Date());
	yield utils.promiseRedis(redisClient, redisClient.hset, redisKeyLicense, redisKeyLicense, val);
}
