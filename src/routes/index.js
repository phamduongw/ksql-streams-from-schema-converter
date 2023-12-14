const express = require('express');
const router = express.Router();

const controllers = require('~/controllers');

router.get('/test', controllers.test);
router.get('/proc-data', controllers.getProcDataByKey);
router.get('/template', controllers.getTemplateByName);
router.get('/template/all', controllers.getAllTemplate);

router.post('/etl-pipeline', controllers.getEtlPipeline);

module.exports = router;
