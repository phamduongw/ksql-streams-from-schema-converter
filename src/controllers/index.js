const services = require('~/services');

exports.test = (req, res) => {
  res.json({ message: 'Hello, World!' });
};

// [GET] /api/etl-data
exports.getEtlData = async (req, res) => {
  let result = await services.getSchema(req.query.schemaName);
  res.status(200).send(result);
};
