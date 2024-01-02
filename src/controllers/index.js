const services = require('~/services');

// [GET] /api/proc-data?{schemaName}
exports.getProcDataByKey = async (req, res) => {
  let result = await services.getSchemaByName(req.query.schemaName);
  res.status(200).send(result);
};

// [POST] /api/etl-pipeline
exports.getEtlPipeline = async (req, res) => {
  const {
    collectionName,
    procName,
    schemaName,
    procType,
    blobDelim,
    procData,
  } = req.body;

  const singleValues = procData.filter(
    (procItem) => procItem['should_parse_sv'],
  );
  const vms = procData.filter((procItem) => procItem['should_parse_vm']);
  const vss = procData.filter((procItem) => procItem['should_parse_vs']);

  let stmtRaw = await services.getTemplateByName(collectionName, 'RAW');
  let stmtMapped = await services.getTemplateByName(collectionName, procType);
  let stmtMultival;
  let stmtSink;

  let sourceStream;
  let selectedFields;
  let listSelectedField;
  let vm;
  let vs;

  if (vms.length) {
    sourceStream = `${schemaName}_MULTIVALUE`;
    stmtSink = await services.getTemplateByName(
      collectionName,
      'SINK_MULTIVALUE',
    );
    stmtMultival = await services.getTemplateByName(
      collectionName,
      'MULTIVALUE',
    );

    listSelectedField = singleValues
      .map(({ name, transformation }) => {
        let output = `DATA.XMLRECORD['${name}']`;
        let fieldName = name.startsWith('LOCALREF_')
          ? name.split('LOCALREF_')[1]
          : name;
        if (/(.*\(.*\))\s([^,]*),*$/.test(transformation)) {
          const matches = transformation.match(/(.*\(.*\))\s([^,]*),*$/);
          fieldName = matches[2];
        } else if (
          transformation.includes('string-join') ||
          /^\[(.*)\]$/.test(transformation)
        ) {
          output = `DATA.XMLRECORD['${name}_multivalue']`;
        }
        return `\t${output} AS ${fieldName},`;
      })
      .join('\n');

    vm = vms.map(({ name }) => `'${name}'`).join(', ');
    vs = vss.map(({ name }) => `'${name}'`).join(', ') || `''`;

    selectedSingle = singleValues.map(({ name, transformation, type }) => {
      let output;
      let fieldName = name.startsWith('LOCALREF_')
        ? name.split('LOCALREF_')[1]
        : name;
      if (name === 'INPUTTER_HIS') {
        output = `SUBSTRING(REGEXP_REPLACE(ARRAY_JOIN(TRANSFORM(REGEXP_SPLIT_TO_ARRAY(REGEXP_REPLACE(DATA.INPUTTER,'^s?[0-9]+:',''), '#(s?[0-9]*:)?'),x => SEAB_FIELD(x,'_',2)),' '),'null ',''),1,4000)`;
        fieldName = 'INPUTTER_HIS';
      } else if (transformation === '') {
        output = `DATA.${name}`;
      } else if (transformation.includes('string-join')) {
        const pattern = /\('*([^']*)'*\)$/;
        if (pattern.test(transformation)) {
          output = `ARRAY_JOIN(FILTER(REGEXP_SPLIT_TO_ARRAY(REGEXP_REPLACE(DATA.${name},'^s?[0-9]+:',''), '#(s?[0-9]+:)?'),(X) => (X <> '')),'${
            transformation.match(pattern)[1]
          }')`;
        } else {
          output = `ARRAY_JOIN(FILTER(REGEXP_SPLIT_TO_ARRAY(REGEXP_REPLACE(DATA.${name},'^s?[0-9]+:',''), '#(s?[0-9]+:)?'),(X) => (X <> '')),' ')`;
        }
      } else if (transformation == 'parse date') {
        output = `PARSE_DATE(DATA.${name}'], 'yyyyMMdd')`;
      } else if (transformation == 'parse timestamp') {
        output = `PARSE_TIMESTAMP(DATA.${name}'], 'yyMMddHHmm')`;
      } else if (transformation == 'substring') {
        output = `SUBSTRING(DATA.${name}'],1,35)`;
      } else if (/^\[(.*)\]$/.test(transformation)) {
        output = `FILTER(REGEXP_SPLIT_TO_ARRAY(DATA.${name}, '(^s?[0-9]+:|#(s?[0-9]+:)?)'), (X) => (X <> ''))[${
          transformation.match(/^\[(.*)\]$/)[1]
        }]`;
      } else if (/(.*\(.*\))\s([^,]*),*$/.test(transformation)) {
        const matches = transformation.match(/(.*\(.*\))\s([^,]*),*$/);
        if (name == 'RECID') {
          output = matches[1].replace('$', `DATA.RECID`);
        } else {
          output = matches[1].replace('$', `DATA.${matches[2]}`);
        }
        fieldName = matches[2];
      } else if (/(.*)\(\[(.*)\](.*)\)$/.test(transformation)) {
        const matches = transformation.match(/(.*)\(\[(.*)\](.*)\)$/);
        if (/[^,\s]/.test(matches[3])) {
          output = `${matches[1]}(FILTER(REGEXP_SPLIT_TO_ARRAY(DATA.${name}, '(^s?[0-9]+:|#(s?[0-9]+:)?)'), (X) => (X <> ''))[${matches[2]}]${matches[3]})`;
        } else {
          output = `${matches[1]}(FILTER(REGEXP_SPLIT_TO_ARRAY(DATA.${name}, '(^s?[0-9]+:|#(s?[0-9]+:)?)'), (X) => (X <> ''))[${matches[2]}], 'yyMMddHHmm')`;
        }
      }
      if (type[1] !== 'string') {
        output = `CAST(${output} AS ${type[1]})`;
      }
      return `\t${output} AS ${fieldName},`;
    });

    selectedMulti = vms.map(({ name, transformation, type }) => {
      let output;
      let fieldName = name.startsWith('LOCALREF_')
        ? name.split('LOCALREF_')[1]
        : name;
      if (name === 'INPUTTER_HIS') {
        output = `SUBSTRING(REGEXP_REPLACE(ARRAY_JOIN(TRANSFORM(REGEXP_SPLIT_TO_ARRAY(REGEXP_REPLACE(DATA.XMLRECORD['INPUTTER_multivalue'],'^s?[0-9]+:',''), '#(s?[0-9]*:)?'),x => SEAB_FIELD(x,'_',2)),' '),'null ',''),1,4000)`;
        fieldName = 'INPUTTER_HIS';
      } else if (transformation === '') {
        output = `XMLRECORD['${name}']`;
      } else if (transformation.includes('string-join')) {
        const pattern = /\('*([^']*)'*\)$/;
        if (pattern.test(transformation)) {
          output = `ARRAY_JOIN(FILTER(REGEXP_SPLIT_TO_ARRAY(REGEXP_REPLACE(DATA.XMLRECORD['${name}_multivalue'],'^s?[0-9]+:',''), '#(s?[0-9]+:)?'),(X) => (X <> '')),'${
            transformation.match(pattern)[1]
          }')`;
        } else {
          output = `ARRAY_JOIN(FILTER(REGEXP_SPLIT_TO_ARRAY(REGEXP_REPLACE(DATA.XMLRECORD['${name}_multivalue'],'^s?[0-9]+:',''), '#(s?[0-9]+:)?'),(X) => (X <> '')),' ')`;
        }
      } else if (transformation == 'parse date') {
        output = `PARSE_DATE(DATA.XMLRECORD['${name}'], 'yyyyMMdd')`;
      } else if (transformation == 'parse timestamp') {
        output = `PARSE_TIMESTAMP(DATA.XMLRECORD['${name}'], 'yyMMddHHmm')`;
      } else if (transformation == 'substring') {
        output = `SUBSTRING(DATA.XMLRECORD['${name}'],1,35)`;
      } else if (/^\[(.*)\]$/.test(transformation)) {
        output = `FILTER(REGEXP_SPLIT_TO_ARRAY(DATA.XMLRECORD['${name}_multivalue'], '(^s?[0-9]+:|#(s?[0-9]+:)?)'), (X) => (X <> ''))[${
          transformation.match(/^\[(.*)\]$/)[1]
        }]`;
      } else if (/(.*\(.*\))\s([^,]*),*$/.test(transformation)) {
        const matches = transformation.match(/(.*\(.*\))\s([^,]*),*$/);
        if (name == 'RECID') {
          output = matches[1].replace('$', `DATA.RECID`);
        } else {
          output = matches[1].replace('$', `DATA.XMLRECORD['${name}']`);
        }
        fieldName = matches[2];
      } else if (/(.*)\(\[(.*)\](.*)\)$/.test(transformation)) {
        const matches = transformation.match(/(.*)\(\[(.*)\](.*)\)$/);
        if (/[^,\s]/.test(matches[3])) {
          output = `${matches[1]}(FILTER(REGEXP_SPLIT_TO_ARRAY(DATA.XMLRECORD['${name}_multivalue'], '(^s?[0-9]+:|#(s?[0-9]+:)?)'), (X) => (X <> ''))[${matches[2]}]${matches[3]})`;
        } else {
          output = `${matches[1]}(FILTER(REGEXP_SPLIT_TO_ARRAY(DATA.XMLRECORD['${name}_multivalue'], '(^s?[0-9]+:|#(s?[0-9]+:)?)'), (X) => (X <> ''))[${matches[2]}], 'yyMMddHHmm')`;
        }
      }
      if (type[1] !== 'string') {
        output = `CAST(${output} AS ${type[1]})`;
      }
      return `\t${output} AS ${fieldName},`;
    });

    selectedFields = selectedSingle.concat(selectedMulti).join('\n');
  } else {
    sourceStream = `${schemaName}_MAPPED`;
    stmtSink = await services.getTemplateByName(collectionName, 'SINK');

    selectedFields = singleValues
      .map(({ name, transformation, type }) => {
        let output;
        let fieldName = name.startsWith('LOCALREF_')
          ? name.split('LOCALREF_')[1]
          : name;
        if (name === 'INPUTTER_HIS') {
          output = `SUBSTRING(REGEXP_REPLACE(ARRAY_JOIN(TRANSFORM(REGEXP_SPLIT_TO_ARRAY(REGEXP_REPLACE(DATA.XMLRECORD['INPUTTER_multivalue'],'^s?[0-9]+:',''), '#(s?[0-9]*:)?'),x => SEAB_FIELD(x,'_',2)),' '),'null ',''),1,4000)`;
          fieldName = 'INPUTTER_HIS';
        } else if (transformation === '') {
          output = `XMLRECORD['${name}']`;
        } else if (transformation.includes('string-join')) {
          const pattern = /\('*([^']*)'*\)$/;
          if (pattern.test(transformation)) {
            output = `ARRAY_JOIN(FILTER(REGEXP_SPLIT_TO_ARRAY(REGEXP_REPLACE(DATA.XMLRECORD['${name}_multivalue'],'^s?[0-9]+:',''), '#(s?[0-9]+:)?'),(X) => (X <> '')),'${
              transformation.match(pattern)[1]
            }')`;
          } else {
            output = `ARRAY_JOIN(FILTER(REGEXP_SPLIT_TO_ARRAY(REGEXP_REPLACE(DATA.XMLRECORD['${name}_multivalue'],'^s?[0-9]+:',''), '#(s?[0-9]+:)?'),(X) => (X <> '')),' ')`;
          }
        } else if (transformation == 'parse date') {
          output = `PARSE_DATE(DATA.XMLRECORD['${name}'], 'yyyyMMdd')`;
        } else if (transformation == 'parse timestamp') {
          output = `PARSE_TIMESTAMP(DATA.XMLRECORD['${name}'], 'yyMMddHHmm')`;
        } else if (transformation == 'substring') {
          output = `SUBSTRING(DATA.XMLRECORD['${name}'],1,35)`;
        } else if (/^\[(.*)\]$/.test(transformation)) {
          output = `FILTER(REGEXP_SPLIT_TO_ARRAY(DATA.XMLRECORD['${name}_multivalue'], '(^s?[0-9]+:|#(s?[0-9]+:)?)'), (X) => (X <> ''))[${
            transformation.match(/^\[(.*)\]$/)[1]
          }]`;
        } else if (/(.*\(.*\))\s([^,]*),*$/.test(transformation)) {
          const matches = transformation.match(/(.*\(.*\))\s([^,]*),*$/);
          if (name == 'RECID') {
            output = matches[1].replace('$', `DATA.RECID`);
          } else {
            output = matches[1].replace('$', `DATA.XMLRECORD['${name}']`);
          }
          fieldName = matches[2];
        } else if (/(.*)\(\[(.*)\](.*)\)$/.test(transformation)) {
          const matches = transformation.match(/(.*)\(\[(.*)\](.*)\)$/);
          if (/[^,\s]/.test(matches[3])) {
            output = `${matches[1]}(FILTER(REGEXP_SPLIT_TO_ARRAY(DATA.XMLRECORD['${name}_multivalue'], '(^s?[0-9]+:|#(s?[0-9]+:)?)'), (X) => (X <> ''))[${matches[2]}]${matches[3]})`;
          } else {
            output = `${matches[1]}(FILTER(REGEXP_SPLIT_TO_ARRAY(DATA.XMLRECORD['${name}_multivalue'], '(^s?[0-9]+:|#(s?[0-9]+:)?)'), (X) => (X <> ''))[${matches[2]}], 'yyMMddHHmm')`;
          }
        }
        if (type[1] !== 'string') {
          output = `CAST(${output} AS ${type[1]})`;
        }
        return `\t${output} AS ${fieldName},`;
      })
      .join('\n');
  }

  stmtRaw = eval('`' + stmtRaw + '`');
  stmtMapped = eval('`' + stmtMapped + '`');
  stmtMultival = stmtMultival && eval('`' + stmtMultival + '`');
  stmtSink = eval('`' + stmtSink + '`');
  res.status(200).send({
    stmtRaw,
    stmtMapped,
    stmtMultival,
    stmtSink,
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
