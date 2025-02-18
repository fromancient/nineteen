const fs = require('fs');
const path = require('path');

// parse .env file correctly
const envFile = fs.readFileSync(path.resolve('.auditor.env'), 'utf8');
const envConfig = Object.fromEntries(
  envFile.split('\n').filter(Boolean).map(line => line.split('='))
);

module.exports = {
  apps: [{
    name: 'auditor',
    script: 'python',
    args: ['-m', 'auditing.audit'],
    env: envConfig
  }]
}