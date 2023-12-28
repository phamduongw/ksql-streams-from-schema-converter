const services = require('~/services');

// [GET] /api/proc-data?{schemaName}
exports.getProcDataByKey = async (req, res) => {
  let result = await services.getSchemaByName(req.query.schemaName);
  res.status(200).send(result);
};

// [POST] /api/etl-pipeline
exports.getEtlPipeline = async (req, res) => {
  var {
    collectionName,
    procName,
    schemaName,
    procType: type,
    blobDelim,
    procData,
  } = req.body;

  var selectedFields = [];
  var sourceStream = null;
  var singleValues = procData.filter((x) => x['should_parse_sv']);
  var multiValues = procData.filter((x) => x['should_parse_mv']);
  var stmtMultival = null;
  var stmtSink = null;

  if (multiValues.length) {
    var stmtMultival = await services.getTemplateByName(
      collectionName,
      'MULTIVALUE',
    );
    var singleValueFields = singleValues
      .map((x) => `\tXMLRECORD['${x.name}'] AS ${x.name}`)
      .join(',\n');
    var multiValueFields = multiValues.map((x) => `'${x.name}'`).join(',');
    stmtMultival = eval('`' + stmtMultival + '`');
    var selectedSingle = singleValues.map((x) => {
      var output;
      if (x.transformation != '') {
        output = x.transformation;
      } else {
        output = x.name;
      }
      if (x.type[1] != 'string') {
        output = `CAST(${output} AS ${x.type[1]})`;
      }
      return `\t${output} AS ${x.name},`;
    });
    var selectedMulti = multiValues.map((x) => {
      var output;
      if (x.transformation != '') {
        output = x.transformation(x.name, `XML_MV['${x.name}']`);
      } else {
        output = `XML_MV['${x.name}']`;
      }
      if (x.type[1] != 'string') {
        output = `CAST(${output} AS ${x.type[1]})`;
      }

      return `\t${output} AS ${x.name},`;
    });
    selectedFields = selectedSingle.concat(selectedMulti).join(',\n');
    sourceStream = `${schemaName}_MULTIVALUE`;
    stmtSink = await services.getTemplateByName(
      collectionName,
      'SINK_MULTIVALUE',
    );
  } else {
    stmtSink = await services.getTemplateByName(collectionName, 'SINK');
    selectedFields = singleValues
      .map((x, index) => {
        var output = '';
        const name = x.name.startsWith('LOCALREF_')
          ? x.name.split('LOCALREF_')[1]
          : x.name;
        if (x.transformation == '') {
          output = `XMLRECORD['${x.name}']`;
        } else if (x.transformation.includes('string-join')) {
          const pattern = /\('*([^']*)'*\)$/;
          if (pattern.test(x.transformation)) {
            output = `ARRAY_JOIN(FILTER(REGEXP_SPLIT_TO_ARRAY(REGEXP_REPLACE(DATA.XMLRECORD['${
              x.name
            }_multivalue'],'^s?[0-9]+:',''), '#(s?[0-9]+:)?'),(X) => (X <> '')),'${
              x.transformation.match(pattern)[1]
            }')`;
          } else {
            output = `ARRAY_JOIN(FILTER(REGEXP_SPLIT_TO_ARRAY(REGEXP_REPLACE(DATA.XMLRECORD['${x.name}_multivalue'],'^s?[0-9]+:',''), '#(s?[0-9]+:)?'),(X) => (X <> '')),' ')`;
          }
        } else if (x.transformation == 'parse date') {
          output = `PARSE_DATE(DATA.XMLRECORD['${x.name}'], 'yyyyMMdd')`;
        } else if (x.transformation == 'parse timestamp') {
          output = `PARSE_TIMESTAMP(DATA.XMLRECORD['${x.name}'], 'yyMMddHHmm')`;
        } else if (x.transformation == 'substring') {
          output = `SUBSTRING(DATA.XMLRECORD['${x.name}'],1,35)`;
        } else if (/^\[(.*)\]$/.test(x.transformation)) {
          output = `FILTER(REGEXP_SPLIT_TO_ARRAY(DATA.XMLRECORD['${
            x.name
          }_multivalue'], '(^s?[0-9]+:|#(s?[0-9]+:)?)'), (X) => (X <> ''))[${
            x.transformation.match(/^\[(.*)\]$/)[1]
          }]`;
        } else if (/(.*\(.*\))\s([^,]*),*$/.test(x.transformation)) {
          const matches = x.transformation.match(/(.*\(.*\))\s([^,]*),*$/);
          if (x.name == 'RECID') {
            output = matches[1].replace('$', `DATA.RECID`);
          } else {
            output = matches[1].replace('$', `DATA.XMLRECORD['${x.name}']`);
          }
          if (x.type[1] != 'string') {
            output = `CAST(${output} AS ${x.type[1]})`;
          }
          return `\t${output} AS ${matches[2]},`;
        }
        if (x.type[1] != 'string') {
          output = `CAST(${output} AS ${x.type[1]})`;
        }
        return `\t${output} AS ${name},`;
      })
      .join(',\n');
    sourceStream = `${schemaName}_MAPPED`;
  }
  var stmtRaw = await services.getTemplateByName(collectionName, 'RAW');
  stmtRaw = eval('`' + stmtRaw + '`');
  var stmtMapped = await services.getTemplateByName(collectionName, type);
  stmtMapped = eval('`' + stmtMapped + '`');
  stmtSink = eval('`' + stmtSink + '`');
  res.status(200).send({
    stmtRaw: stmtRaw,
    stmtMapped: stmtMapped,
    stmtMultival: stmtMultival,
    stmtSink: stmtSink,
  });
};

// [GET] /api/template/all?{collectionName}
exports.getAllTemplates = async (req, res) => {
  let result = await services.getAllTemplates(req.query.collectionName);
  res.status(200).send(result);
};

// [PUT] /api/template/all
exports.updateAllTemplates = async (req, res) => {
  const { collectionName, templateData } = req.body;
  await services.updateAllTemplates(collectionName, templateData);
  res.status(200).send({ status: 'success' });
};

// [GET] /api/template?{templateName}
exports.getTemplateByName = async (req, res) => {
  const { collectionName, templateName } = req.query;
  let result = await services.getTemplateByName(collectionName, templateName);
  res.status(200).send(result);
};

// [POST] /api/execute/{query}
exports.executeQuery = async (req, res) => {
  let result = await services.executeQuery(req.body.query);
  res.status(200).send(result);
};

// [POST] /api/createTestData?{collectionName}
exports.createTestData = async (req, res) => {
  let result = await services.createTestData(req.query.collectionName);
  res.status(200).send(result);
};
