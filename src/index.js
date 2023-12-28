require('module-alias/register');

const express = require('express');
const path = require('path');
const morgan = require('morgan');
const cors = require('cors');

const route = require('~/routes');

const { connectToCouchbase } = require('~/services');

const app = express();
const port = 80;

// Use Static Folder
app.use(express.static(path.join(__dirname, 'public')));

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
