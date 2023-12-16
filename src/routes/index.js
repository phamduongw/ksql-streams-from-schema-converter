const express = require('express');
const router = express.Router();

const controllers = require('~/controllers');

router.get('/proc-data', controllers.getProcDataByKey);
router.get('/template', controllers.getTemplateByName);
router.get('/template/all', controllers.getAllTemplates);
router.put('/template/all', controllers.updateAllTemplates);
router.post('/etl-pipeline', controllers.getEtlPipeline);
router.post('/execute', controllers.executeQuery);
router.get('/createTestData', controllers.createTestData);

module.exports = router;
