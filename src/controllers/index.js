const services = require('~/services');

exports.test = (req, res) => {
  res.json({ message: 'Hello, World!' });
};

// [GET] /api/proc-data/{schemaName}
exports.getProcDataByKey = async (req, res) => {
  let result = await services.getSchemaByName(req.query.schemaName);
  res.status(200).send(result);
};

// [POST] /api/etl-pipeline
exports.getEtlPipeline = async (req, res) => {
  var {
    procName,
    schemaName,
    procType: type,
    blobDelimiter: blobDelim,
    procData,
  } = req.body;

  var selectedFields = [];
  var sourceStream = null;
  var singleValues = procData.filter((x) => x['should_parse_sv']);
  var multiValues = procData.filter((x) => x['should_parse_mv']);
  var stmtMultival = null;
  var stmtSink = null;

  if (multiValues.length) {
    var stmtMultival = await services.getTemplateByName('MULTIVALUE');
    var singleValueFields = singleValues
      .map((x) => `XMLRECORD['${x.name}'] AS ${x.name}`)
      .join(',\n');

    stmtMultival = eval('`' + stmtMultival + '`');
    var selectedSingle = singleValues.map((x) => {
      var output;
      if (x.transformation != '') {
        output = x.transformation;
      } else {
        output = x.name;
      }
      if (x.type != 'string') {
        output = `CAST(${output} AS ${x.type})`;
      }
      return `${output} AS ${x.name}`;
    });
    var selectedMulti = multivalues.map((x) => {
      var output;
      if (x.transformation != '') {
        output = x.transformation(x.name, `XML_MV['${x.name}']`);
      } else {
        output = `XML_MV['${x.name}']`;
      }
      if (x.type != 'string') {
        output = `CAST(${output} AS ${x.type})`;
      }

      return `${output} AS ${x.name}`;
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
        if (x.type != 'string') {
          output = `CAST(${output} AS ${x.type})`;
        }
        return `${output} as ${name}`;
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
exports.getAllTemplate = async (req, res) => {
  let result = await services.getAllTemplate();
  res.status(200).send(result);
};

// [GET] /api/templates/{templateName}
exports.getTemplateByName = async (req, res) => {
  let result = await services.getTemplateByName(req.query.templateName);
  res.status(200).send(result);
};
