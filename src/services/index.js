const dotenv = require('dotenv');
const couchbase = require('couchbase');

dotenv.config();

let cluster;
let bucket;
let scope;

const connectToCouchbase = async () => {
  const connectionOptions = {
    username: process.env.COUCHBASE_USERNAME,
    password: process.env.COUCHBASE_PASSWORD,
    configProfile: 'wanDevelopment',
  };

  try {
    cluster = await couchbase.connect(
      process.env.COUCHBASE_URL,
      connectionOptions,
    );
    bucket = cluster.bucket(process.env.COUCHBASE_BUCKET);
    scope = bucket.scope(process.env.COUCHBASE_SCOPE);
    console.log('Connected to Couchbase');
  } catch (error) {
    console.error('Error connecting to Couchbase:', error);
    throw error;
  }
};

const getSchemaByName = async (name) => {
  try {
    const query =
      'SELECT meta().id as schema_name, fields FROM `schema` USE KEYS [$1]';
    const result = await scope.query(query, { parameters: [name] });
    return result.rows[0];
  } catch (error) {
    console.error('Error getting schema:', error);
    throw error;
  }
};

const getAllTemplates = async (collectionName) => {
  try {
    const query = `SELECT meta().id as template_name, template as template from \`${collectionName}\``;
    const result = await scope.query(query);
    return result.rows;
  } catch (error) {
    console.error('Error getting schema:', error);
    throw error;
  }
};

const updateAllTemplates = async (collectionName, templates) => {
  const collection = scope.collection(collectionName);
  const updatePromises = templates.map(async ({ template_name, template }) => {
    try {
      if (template) {
        await collection.upsert(template_name, { template });
      } else {
        await collection.remove(template_name);
      }
    } catch (error) {
      console.error(`Error updating template ${template_name}:`, error);
    }
  });
  await Promise.all(updatePromises);
};

const getTemplateByName = async (collectionName, templateName) => {
  try {
    const query = `SELECT template from \`${collectionName}\` USE KEYS [$1]`;
    const result = await scope.query(query, { parameters: [templateName] });
    return result.rows[0].template.replace(/`/g, '\\`');
  } catch (error) {
    console.error('Error getting schema:', error);
    throw error;
  }
};

// Test query
const executeQuery = async (query) => {
  try {
    const result = await scope.query(query);
    return result.rows;
  } catch (error) {
    console.error('Error executing query:', error);
    throw error;
  }
};

// Test data
const createTestData = async (collectionName) => {
  const result = await updateAllTemplates(
    collectionName,
    await getAllTemplates(),
  );
  return result;
};

module.exports = {
  connectToCouchbase,
  getSchemaByName,
  getAllTemplates,
  updateAllTemplates,
  getTemplateByName,
  executeQuery,
  createTestData,
};
