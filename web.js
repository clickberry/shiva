var express = require('express');
var path = require('path');
var bodyParser = require('body-parser');
var logger = require('morgan');
var cors = require('cors');

var routes = require('./routes');
var api = require('./routes/api');

var app = express();

// enable CORS
app.use(cors({ allowedHeaders: 'Authorization, Content-Type' }));

// view engine setup
app.set('views', path.join(__dirname, 'views'));
app.set('view engine', 'pug');

// verbose logger for dev
if (app.get('env') === 'development') {
  app.use(logger('dev'));
} else {
  app.use(logger());
}

// body parser
app.use(bodyParser.json());
app.use(bodyParser.urlencoded({ extended: true }));

// api
app.use('/', api);

// error handlers
app.use(routes.notfound);
app.use(routes.error);

module.exports = app;
