require('module-alias/register');

const express = require('express');
const morgan = require('morgan');
const cors = require('cors');

const route = require('~/routes');

const { connectToCouchbase } = require('~/services');

const app = express();
const port = 80;

// HTTP Logger
app.use(morgan('combined'));

// CORS
app.use(cors());

// JSON
app.use(express.json());

// Routes
app.use('/api', route);

// Listener
app.listen(port, async () => {
  await connectToCouchbase();
  console.log(`App listening on port ${port}`);
});
