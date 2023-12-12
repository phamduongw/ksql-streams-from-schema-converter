const express = require('express');
const router = express.Router();

const controllers = require('~/controllers');

router.get('/test', controllers.test);
router.get('/etl-data', controllers.getEtlData);

module.exports = router;
