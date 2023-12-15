const services = require('~/services');

// [GET] /api/proc-data/{schemaName}
exports.getProcDataByKey = async (req, res) => {
  let result = await services.getSchemaByName(req.query.schemaName);
  res.status(200).send(result);
};

// [POST] /api/etl-pipeline
exports.getEtlPipeline = async (req, res) => {
  var { procName, schemaName, procType: type, blobDelim, procData } = req.body;

  var selectedFields = [];
  var sourceStream = null;
  var singleValues = procData.filter((x) => x['should_parse_sv']);
  var multiValues = procData.filter((x) => x['should_parse_mv']);
  var stmtMultival = null;
  var stmtSink = null;

  if (multiValues.length) {
    var stmtMultival = await services.getTemplateByName('MULTIVALUE');
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
      return `\t${output} AS ${x.name}`;
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

      return `\t${output} AS ${x.name}`;
    });
    selectedFields = selectedSingle.concat(selectedMulti).join(',\n');
    sourceStream = `${schemaName}_MULTIVALUE`;
    stmtSink = await services.getTemplateByName('SINK_MULTIVALUE');
  } else {
    stmtSink = await services.getTemplateByName('SINK');
    selectedFields = singleValues
      .map((x, index) => {
        var output = '';
        const name = x.name.startsWith('LOCALREF_')
          ? x.name.split('LOCALREF_')[1]
          : x.name;
        if (x.transformation == '') {
          output = `XMLRECORD['${x.name}']`;
        } else if (x.transformation == 'string-join') {
          output = `TRIM(CONCAT(DATA.XMLRECORD['${x.name}'], REGEXP_REPLACE(DATA.XMLRECORD['${x.name}_multivalue'], '(^s?1|#s?[0-9]+):', ' ')))`;
        } else if (x.transformation == 'parse date') {
          output = `PARSE_DATE(DATA.XMLRECORD['${x.name}'], 'yyyyMMdd') ${x.name}`;
        } else if (x.transformation == 'parse timestamp') {
          output = `PARSE_TIMESTAMP(DATA.XMLRECORD['${x.name}'], 'yyMMddHHmm') ${x.name}`;
        }
        if (x.type[1] != 'string') {
          output = `CAST(${output} AS ${x.type[1]})`;
        }
        return `\t${output} AS ${name}`;
      })
      .join(',\n');
    sourceStream = `${schemaName}_MAPPED`;
  }
  var stmtRaw = await services.getTemplateByName('RAW');
  stmtRaw = eval('`' + stmtRaw + '`');
  var stmtMapped = await services.getTemplateByName(type);
  stmtMapped = eval('`' + stmtMapped + '`');
  stmtSink = eval('`' + stmtSink + '`');
  res.status(200).send({
    stmtRaw: stmtRaw,
    stmtMapped: stmtMapped,
    stmtMultival: stmtMultival,
    stmtSink: stmtSink,
  });
};

// [GET] /api/template/all
exports.getAllTemplates = async (req, res) => {
  let result = await services.getAllTemplates();
  res.status(200).send(result);
};

// [PUT] /api/template/all
exports.updateAllTemplates = async (req, res) => {
  await services.updateAllTemplates(req.body.templateData);
  res.status(200).send({ status: 'success' });
};

// [GET] /api/template/{templateName}
exports.getTemplateByName = async (req, res) => {
  let result = await services.getTemplateByName(req.query.templateName);
  res.status(200).send(result);
};
