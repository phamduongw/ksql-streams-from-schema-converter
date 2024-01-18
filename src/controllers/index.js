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

  // Proc data
  const singleValues = procData.filter(
    (procItem) => procItem['should_parse_sv'],
  );
  const vms = procData.filter((procItem) => procItem['should_parse_vm']);
  const vss = procData.filter((procItem) => procItem['should_parse_vs']);

  // Stmt
  let stmtRaw;
  let stmtMapped;
  let stmtMultival;
  let stmtSink;
  let stmtDdl;

  // Template
  let sourceStream;
  let selectedFields;
  let listSelectedField;
  let vm;
  let vs;

  // Added field comment
  const addedFieldComment = (alias, doc, name) => {
    const aliasPart = alias.match(/^c(\d*)(?:_m(\d*))*$/);
    try {
      return ` -- add field ${name} - ${alias} || INSERT INTO T24BNK.STANDARD_SELECTION_MANUAL (PREFIX, TABLE_NAME, FIELD_NAME, FIELD_FM, FIELD_VM, FIELD_SINGLE_MULTI, DATA_TYPE) VALUES('FBNK', '${schemaName.replace(
        /_/g,
        '.',
      )}', '${name.replace(/_/g, '.')}', ${aliasPart[1]}, ${
        aliasPart[2] || null
      }, '${doc || 'S'}', 'VARCHAR2'); commit;`;
    } catch {
      return ' -- INPUT FORMAT ERROR';
    }
  };

  // Parser
  const singleParser = ({
    aliases,
    doc,
    name,
    transformation,
    type,
    nested,
    isAddedField,
  }) => {
    let output;
    let fieldName = name.startsWith('LOCALREF_')
      ? name.split('LOCALREF_')[1]
      : name;
    if (name === 'INPUTTER_HIS') {
      output = `SUBSTRING(REGEXP_REPLACE(ARRAY_JOIN(TRANSFORM(REGEXP_SPLIT_TO_ARRAY(REGEXP_REPLACE(DATA.XMLRECORD['INPUTTER_multivalue'],'^s?[0-9]+:',''), '#(s?[0-9]*:)?'),x => SEAB_FIELD(x,'_',2)),' '),'null ',''),1,4000)`;
      fieldName = 'INPUTTER_HIS';
    } else if (transformation === '') {
      output = `DATA.XMLRECORD['${name}']`;
    } else if (transformation.includes('string-join')) {
      const pattern = /\('*([^']*)'*\)$/;
      if (pattern.test(transformation)) {
        output = `ARRAY_JOIN(FILTER(REGEXP_SPLIT_TO_ARRAY(REGEXP_REPLACE(DATA.XMLRECORD['${name}_multivalue'],'^s?[0-9]+:',''), '#(s?[0-9]+:)?'),(X) => (X <> '')),'${
          transformation.match(pattern)[1]
        }')`;
      } else {
        output = `ARRAY_JOIN(FILTER(REGEXP_SPLIT_TO_ARRAY(REGEXP_REPLACE(DATA.XMLRECORD['${name}_multivalue'],'^s?[0-9]+:',''), '#(s?[0-9]+:)?'),(X) => (X <> '')),' ')`;
      }
    } else if (transformation == 'parse_date') {
      output = `PARSE_DATE(DATA.XMLRECORD['${name}'], 'yyyyMMdd')`;
    } else if (transformation == 'parse_timestamp') {
      output = `PARSE_TIMESTAMP(DATA.XMLRECORD['${name}'], 'yyMMddHHmm')`;
    } else if (transformation == 'substring') {
      output = `SUBSTRING(DATA.XMLRECORD['${name}'],1,35)`;
    } else if (transformation === 'seab_field') {
      output = `SEAB_FIELD(DATA.XMLRECORD['${name}'],'_',2)`;
    } else if (/^\[(.*)\]$/.test(transformation)) {
      output = `FILTER(REGEXP_SPLIT_TO_ARRAY(DATA.XMLRECORD['${name}_multivalue'], '(^s?[0-9]+:|#(s?[0-9]+:)?)'), (X) => (X <> ''))[${
        transformation.match(/^\[(.*)\]$/)[1]
      }]`;
    } else if (/^([^\s(]*)\((.*)\)\s*(.*)$/.test(transformation)) {
      const matches = transformation.match(/^([^\s(]*)\((.*)\)\s*(.*)$/);

      let field = `DATA.XMLRECORD['${name}']`;

      fieldName = matches[3];
      matches[1] = matches[1].toUpperCase();
      if (matches[2].includes('$')) {
        if (name === 'RECID') {
          field = 'DATA.RECID';
        } else if (transformation.includes('string-join')) {
          field = `DATA.XMLRECORD['${name}_multivalue']`;
        }

        if (/\$\$/g.test(matches[2])) {
          output = `${matches[1]}(${matches[2].replace(/\$\$/g, name)})`;
        } else {
          output = `${matches[1]}(${matches[2].replace(/\$/g, field)})`;
        }
      } else if (/^\[.*\](.*)$/.test(matches[2])) {
        const matches2 = matches[2].match(/^\[(.*)\](.*)$/);

        let field = `DATA.XMLRECORD['${name}_multivalue']`;
        let params;

        if (transformation.includes('parse_date')) {
          params = `, 'yyyyMMdd'`;
        } else if (transformation.includes('parse_timestamp')) {
          params = `, 'yyMMddHHmm'`;
        } else if (transformation.includes('substring')) {
          params = `,1,35`;
        } else if (transformation.includes('seab_field')) {
          params = `,'_',2`;
        }

        if (name === 'RECID') {
          field = 'DATA.RECID';
        }

        if (/[^,\s]/.test(matches2[2])) {
          params = matches2[2];
        }

        output = `${
          matches[1]
        }(FILTER(REGEXP_SPLIT_TO_ARRAY(${field}, '(^s?[0-9]+:|#(s?[0-9]+:)?)'), (X) => (X <> ''))[${
          matches2[1]
        }]${params || ''})`;
      }
    } else {
      return `\t${transformation}`;
    }

    if (nested.includes('$')) {
      const matches = nested.match(/(^.*\))\s*(.*$)/);
      output = matches[1].replace(/\$/g, output);
      fieldName = matches[2] || fieldName;
    }

    if (type[1] !== 'string') {
      output = `CAST(${output} AS ${type[1]})`;
    }

    let comment = '';

    if (isAddedField) {
      if (aliases[0]) {
        comment = addedFieldComment(aliases[0], doc, name);
      } else {
        comment = ` -- add field ${name} AS ${fieldName.toUpperCase() || name}`;
      }
    }

    return `\t${output} AS ${fieldName.toUpperCase() || name} ,${comment}`;
  };

  const multiParser = ({
    aliases,
    doc,
    name,
    transformation,
    type,
    nested,
    isAddedField,
  }) => {
    let output;
    let fieldName = name.startsWith('LOCALREF_')
      ? name.split('LOCALREF_')[1]
      : name;
    if (name === 'INPUTTER_HIS') {
      output = `SUBSTRING(REGEXP_REPLACE(ARRAY_JOIN(TRANSFORM(REGEXP_SPLIT_TO_ARRAY(REGEXP_REPLACE(DATA.XMLRECORD['INPUTTER'],'^s?[0-9]+:',''), '#(s?[0-9]*:)?'),x => SEAB_FIELD(x,'_',2)),' '),'null ',''),1,4000)`;
      fieldName = 'INPUTTER_HIS';
    } else if (transformation === '') {
      output = `DATA.XMLRECORD['${name}']`;
    } else if (transformation.includes('string-join')) {
      const pattern = /\('*([^']*)'*\)$/;
      if (pattern.test(transformation)) {
        output = `ARRAY_JOIN(FILTER(REGEXP_SPLIT_TO_ARRAY(REGEXP_REPLACE(DATA.XMLRECORD['${name}'],'^s?[0-9]+:',''), '#(s?[0-9]+:)?'),(X) => (X <> '')),'${
          transformation.match(pattern)[1]
        }')`;
      } else {
        output = `ARRAY_JOIN(FILTER(REGEXP_SPLIT_TO_ARRAY(REGEXP_REPLACE(DATA.XMLRECORD['${name}'],'^s?[0-9]+:',''), '#(s?[0-9]+:)?'),(X) => (X <> '')),' ')`;
      }
    } else if (transformation == 'parse_date') {
      output = `PARSE_DATE(DATA.XMLRECORD['${name}'], 'yyyyMMdd')`;
    } else if (transformation == 'parse_timestamp') {
      output = `PARSE_TIMESTAMP(DATA.XMLRECORD['${name}'], 'yyMMddHHmm')`;
    } else if (transformation == 'substring') {
      output = `SUBSTRING(DATA.XMLRECORD['${name}'],1,35)`;
    } else if (transformation === 'seab_field') {
      output = `SEAB_FIELD(DATA.XMLRECORD['${name}'],'_',2)`;
    } else if (/^\[(.*)\]$/.test(transformation)) {
      output = `FILTER(REGEXP_SPLIT_TO_ARRAY(DATA.XMLRECORD['${name}'], '(^s?[0-9]+:|#(s?[0-9]+:)?)'), (X) => (X <> ''))[${
        transformation.match(/^\[(.*)\]$/)[1]
      }]`;
    } else if (/^([^\s(]*)\((.*)\)\s*(.*)$/.test(transformation)) {
      const matches = transformation.match(/^([^\s(]*)\((.*)\)\s*(.*)$/);
      fieldName = matches[3];
      matches[1] = matches[1].toUpperCase();
      if (matches[2].includes('$')) {
        if (name === 'RECID') {
          if (/\$\$/g.test(matches[2])) {
            output = `${matches[1]}(${matches[2].replace(/\$\$/g, name)})`;
          } else {
            output = `${matches[1]}(${matches[2].replace(
              /\$/g,
              `DATA.RECID`,
            )})`;
          }
        } else {
          if (/\$\$/g.test(matches[2])) {
            output = `${matches[1]}(${matches[2].replace(/\$\$/g, name)})`;
          } else {
            output = `${matches[1]}(${matches[2].replace(
              /\$/g,
              `DATA.XMLRECORD['${name}']`,
            )})`;
          }
        }
        fieldName = matches[3];
      } else if (/^\[.*\](.*)$/.test(matches[2])) {
        const matches2 = matches[2].match(/^\[(.*)\](.*)$/);

        let field = `DATA.XMLRECORD['${name}']`;
        let params;

        if (transformation.includes('parse_date')) {
          params = `, 'yyyyMMdd'`;
        } else if (transformation.includes('parse_timestamp')) {
          params = `, 'yyMMddHHmm'`;
        } else if (transformation.includes('substring')) {
          params = `,1,35`;
        } else if (transformation.includes('seab_field')) {
          params = `,'_',2`;
        }

        if (name === 'RECID') {
          field = 'DATA.RECID';
        }

        if (/[^,\s]/.test(matches2[2])) {
          params = matches2[2];
        }

        output = `${
          matches[1]
        }(FILTER(REGEXP_SPLIT_TO_ARRAY(${field}, '(^s?[0-9]+:|#(s?[0-9]+:)?)'), (X) => (X <> ''))[${
          matches2[1]
        }]${params || ''})`;
      }
    } else {
      return `\t${transformation}`;
    }

    if (nested.includes('$')) {
      const matches = nested.match(/(^.*\))\s*(.*$)/);
      output = matches[1].replace(/\$/g, output);
      fieldName = matches[2] || fieldName;
    }

    if (type[1] !== 'string') {
      output = `CAST(${output} AS ${type[1]})`;
    }

    let comment = '';

    if (isAddedField) {
      if (aliases[0]) {
        comment = addedFieldComment(aliases[0], doc, name);
      } else {
        comment = ` -- add field ${name} AS ${fieldName.toUpperCase() || name}`;
      }
    }

    return `\t${output} AS ${fieldName.toUpperCase() || name} ,${comment}`;
  };

  const singleSplitBlobParser = ({
    aliases,
    doc,
    name,
    transformation,
    type,
    nested,
    isAddedField,
  }) => {
    let output;
    let fieldName = name.startsWith('LOCALREF_')
      ? name.split('LOCALREF_')[1]
      : name;
    if (transformation === '') {
      output = `SEAB_HEXTOTEXT(FROM_BYTES(DATA.XMLRECORD->VALUE, 'hex'))`;
    } else if (transformation.includes('string-join')) {
      const pattern = /\('*([^']*)'*\)$/;
      if (pattern.test(transformation)) {
        output = `ARRAY_JOIN(FILTER(REGEXP_SPLIT_TO_ARRAY(REGEXP_REPLACE(SEAB_HEXTOTEXT(FROM_BYTES(DATA.XMLRECORD->VALUE, 'hex')),'^s?[0-9]+:',''), '#(s?[0-9]+:)?'),(X) => (X <> '')),'${
          transformation.match(pattern)[1]
        }')`;
      } else {
        output = `ARRAY_JOIN(FILTER(REGEXP_SPLIT_TO_ARRAY(REGEXP_REPLACE(SEAB_HEXTOTEXT(FROM_BYTES(DATA.XMLRECORD->VALUE, 'hex')),'^s?[0-9]+:',''), '#(s?[0-9]+:)?'),(X) => (X <> '')),' ')`;
      }
    } else if (transformation == 'parse_date') {
      output = `PARSE_DATE(SEAB_HEXTOTEXT(FROM_BYTES(DATA.XMLRECORD->VALUE, 'hex')), 'yyyyMMdd')`;
    } else if (transformation == 'parse_timestamp') {
      output = `PARSE_TIMESTAMP(SEAB_HEXTOTEXT(FROM_BYTES(DATA.XMLRECORD->VALUE, 'hex')), 'yyMMddHHmm')`;
    } else if (transformation == 'substring') {
      output = `SUBSTRING(SEAB_HEXTOTEXT(FROM_BYTES(DATA.XMLRECORD->VALUE, 'hex')),1,35)`;
    } else if (transformation === 'seab_field') {
      output = `SEAB_FIELD(SEAB_HEXTOTEXT(FROM_BYTES(DATA.XMLRECORD->VALUE, 'hex')),'_',2)`;
    } else if (/^\[(.*)\]$/.test(transformation)) {
      output = `FILTER(REGEXP_SPLIT_TO_ARRAY(SEAB_HEXTOTEXT(FROM_BYTES(DATA.XMLRECORD->VALUE, 'hex')), '(^s?[0-9]+:|#(s?[0-9]+:)?)'), (X) => (X <> ''))[${
        transformation.match(/^\[(.*)\]$/)[1]
      }]`;
    } else if (/^([^\s(]*)\((.*)\)\s*(.*)$/.test(transformation)) {
      const matches = transformation.match(/^([^\s(]*)\((.*)\)\s*(.*)$/);

      let field = `SEAB_HEXTOTEXT(FROM_BYTES(DATA.XMLRECORD->VALUE, 'hex'))`;

      fieldName = matches[3];
      matches[1] = matches[1].toUpperCase();
      if (matches[2].includes('$')) {
        if (name === 'RECID') {
          field = 'DATA.RECID';
        } else if (transformation.includes('string-join')) {
          field = `SEAB_HEXTOTEXT(FROM_BYTES(DATA.XMLRECORD->VALUE, 'hex'))`;
        }

        if (/\$\$/g.test(matches[2])) {
          output = `${matches[1]}(${matches[2].replace(/\$\$/g, name)})`;
        } else {
          output = `${matches[1]}(${matches[2].replace(/\$/g, field)})`;
        }
      } else if (/^\[.*\](.*)$/.test(matches[2])) {
        const matches2 = matches[2].match(/^\[(.*)\](.*)$/);

        let field = `SEAB_HEXTOTEXT(FROM_BYTES(DATA.XMLRECORD->VALUE, 'hex'))`;
        let params;

        if (transformation.includes('parse_date')) {
          params = `, 'yyyyMMdd'`;
        } else if (transformation.includes('parse_timestamp')) {
          params = `, 'yyMMddHHmm'`;
        } else if (transformation.includes('substring')) {
          params = `,1,35`;
        } else if (transformation.includes('seab_field')) {
          params = `,'_',2`;
        }

        if (name === 'RECID') {
          field = 'DATA.RECID';
        }

        if (/[^,\s]/.test(matches2[2])) {
          params = matches2[2];
        }

        output = `${
          matches[1]
        }(FILTER(REGEXP_SPLIT_TO_ARRAY(${field}, '(^s?[0-9]+:|#(s?[0-9]+:)?)'), (X) => (X <> ''))[${
          matches2[1]
        }]${params || ''})`;
      }
    } else {
      return `\t${transformation}`;
    }

    if (nested.includes('$')) {
      const matches = nested.match(/(^.*\))\s*(.*$)/);
      output = matches[1].replace(/\$/g, output);
      fieldName = matches[2] || fieldName;
    }

    if (type[1] !== 'string') {
      output = `CAST(${output} AS ${type[1]})`;
    }

    let comment = '';

    if (isAddedField) {
      if (aliases[0]) {
        comment = addedFieldComment(aliases[0], doc, name);
      } else {
        comment = ` -- add field ${name} AS ${fieldName.toUpperCase() || name}`;
      }
    }

    return `\t${output} AS ${fieldName.toUpperCase() || name} ,${comment}`;
  };

  // Handler
  const singleHandler = async () => {
    stmtSink = await services.getTemplateByName(collectionName, 'SINK');
    stmtDdl = await services.getTemplateByName(collectionName, 'DDL_SINGLE');
    sourceStream = `${schemaName}_MAPPED`;
    selectedFields = singleValues.map(singleParser).join('\n');
  };

  const multiHandler = async () => {
    stmtSink = await services.getTemplateByName(
      collectionName,
      'SINK_MULTIVALUE',
    );
    stmtMultival = await services.getTemplateByName(
      collectionName,
      'MULTIVALUE',
    );
    sourceStream = `${schemaName}_MULTIVALUE`;

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
          /^\[(.*)\]$/.test(transformation) ||
          /(.*)\(\[(.*)\](.*)\)/.test(transformation)
        ) {
          output = `DATA.XMLRECORD['${name}_multivalue']`;
        }
        return `\t${output} AS ${fieldName.toUpperCase() || name},`;
      })
      .join('\n');

    vm = vms.map(({ name }) => `'${name}'`).join(', ') || `''`;
    vs = vss.map(({ name }) => `'${name}'`).join(', ') || `''`;

    selectedSingle = singleValues.map(
      ({ aliases, doc, name, transformation, type, nested, isAddedField }) => {
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
        } else if (transformation == 'parse_date') {
          output = `PARSE_DATE(DATA.${name}, 'yyyyMMdd')`;
        } else if (transformation == 'parse_timestamp') {
          output = `PARSE_TIMESTAMP(DATA.${name}, 'yyMMddHHmm')`;
        } else if (transformation == 'substring') {
          output = `SUBSTRING(DATA.${name},1,35)`;
        } else if (transformation === 'seab_field') {
          output = `SEAB_FIELD(DATA.${name},'_',2)`;
        } else if (/^\[(.*)\]$/.test(transformation)) {
          output = `FILTER(REGEXP_SPLIT_TO_ARRAY(DATA.${name}, '(^s?[0-9]+:|#(s?[0-9]+:)?)'), (X) => (X <> ''))[${
            transformation.match(/^\[(.*)\]$/)[1]
          }]`;
        } else if (/^([^\s(]*)\((.*)\)\s*(.*)$/.test(transformation)) {
          const matches = transformation.match(/^([^\s(]*)\((.*)\)\s*(.*)$/);
          fieldName = matches[3];
          matches[1] = matches[1].toUpperCase();
          if (matches[2].includes('$')) {
            if (name === 'RECID') {
              if (/\$\$/g.test(matches[2])) {
                output = `${matches[1]}(${matches[2].replace(/\$\$/g, name)})`;
              } else {
                output = `${matches[1]}(${matches[2].replace(
                  /\$/g,
                  `DATA.RECID`,
                )})`;
              }
            } else {
              if (/\$\$/g.test(matches[2])) {
                output = `${matches[1]}(${matches[2].replace(/\$\$/g, name)})`;
              } else {
                output = `${matches[1]}(${matches[2].replace(
                  /\$/g,
                  `DATA.${name}`,
                )})`;
              }
            }
            fieldName = matches[3];
          } else if (/^\[.*\](.*)$/.test(matches[2])) {
            const matches2 = matches[2].match(/^\[(.*)\](.*)$/);

            let field = `DATA.${name}`;
            let params;

            if (transformation.includes('parse_date')) {
              params = `, 'yyyyMMdd'`;
            } else if (transformation.includes('parse_timestamp')) {
              params = `, 'yyMMddHHmm'`;
            } else if (transformation.includes('substring')) {
              params = `,1,35`;
            } else if (transformation.includes('seab_field')) {
              params = `,'_',2`;
            }
            if (name === 'RECID') {
              field = 'DATA.RECID';
            }

            if (/[^,\s]/.test(matches2[2])) {
              params = matches2[2];
            }
            output = `${
              matches[1]
            }(FILTER(REGEXP_SPLIT_TO_ARRAY(${field}, '(^s?[0-9]+:|#(s?[0-9]+:)?)'), (X) => (X <> ''))[${
              matches2[1]
            }]${params || ''})`;
          }
        } else {
          return `\t${transformation}`;
        }

        if (nested.includes('$')) {
          const matches = nested.match(/(^.*\))\s*(.*$)/);
          output = matches[1].replace(/\$/g, output);
          fieldName = matches[2] || fieldName;
        }

        if (type[1] !== 'string') {
          output = `CAST(${output} AS ${type[1]})`;
        }

        let comment = '';

        if (isAddedField) {
          if (aliases[0]) {
            comment = addedFieldComment(aliases[0], doc, name);
          } else {
            comment = ` -- add field ${name} AS ${
              fieldName.toUpperCase() || name
            }`;
          }
        }

        return `\t${output} AS ${fieldName.toUpperCase() || name} ,${comment}`;
      },
    );
    selectedMulti = vms.map(multiParser);
    selectedVS = vss.map(multiParser);
    selectedFields = selectedSingle
      .concat(selectedMulti)
      .concat(selectedVS)
      .join('\n');
    stmtDdl = await services.getTemplateByName(
      collectionName,
      'DDL_MULTIVALUE',
    );
  };

  if (procType === 'XML') {
    stmtRaw = await services.getTemplateByName(collectionName, 'RAW');
    stmtMapped = await services.getTemplateByName(collectionName, 'XML');

    if (vms.length || vss.length) {
      await multiHandler();
    } else {
      await singleHandler();
    }
  } else if (procType === 'BLOB') {
    stmtRaw = await services.getTemplateByName(collectionName, 'BLOB_RAW');

    if (blobDelim === 'FE') {
      stmtMapped = await services.getTemplateByName(
        collectionName,
        'BLOB_PARSE_T24',
      );
      await singleHandler();
    } else if (blobDelim === 'FEFD') {
      stmtMapped = await services.getTemplateByName(
        collectionName,
        'BLOB_PARSE_T24',
      );
      if (vms.length || vss.length) {
        await multiHandler();
      } else {
        await singleHandler();
      }
    } else if (blobDelim === 'SPLIT') {
      stmtMapped = await services.getTemplateByName(
        collectionName,
        'BLOB_SPLIT',
      );
      await singleHandler();
      stmtSink = await services.getTemplateByName(collectionName, 'SINK_BLOB');
      selectedFields = singleValues.map(singleSplitBlobParser).join('\n');
    }
  }
  stmtRaw = eval('`' + stmtRaw + '`');
  stmtMapped = eval('`' + stmtMapped + '`');
  stmtMultival = stmtMultival && eval('`' + stmtMultival + '`');
  stmtSink = eval('`' + stmtSink + '`');
  stmtDdl = eval('`' + stmtDdl + '`');
  res.status(200).send({
    stmtRaw,
    stmtMapped,
    stmtMultival,
    stmtSink,
    stmtDdl,
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
