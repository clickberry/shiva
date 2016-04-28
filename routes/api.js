// env
if (!process.env.S3_BUCKET) {
  console.log("S3_BUCKET environment variable required.");
  process.exit(1);
}
var bucket = process.env.S3_BUCKET;

var express = require('express');
var router = express.Router();
var debug = require('debug')('clickberry:shiva:web');
var busboy = require('busboy');
var uuid = require('node-uuid');
var path = require('path');
var multiparty = require('multiparty');
var AWS = require('aws-sdk');
var s3 = new AWS.S3();

var presets = require('../lib/presets');
var Bus = require('../lib/bus');
var bus = new Bus();
var shivaUtils = require('../lib/utils');

/**
 * Healthcheck endpoint
 */
router.get('/heartbeat', function (req, res) {
  res.send();
});

/**
 * Helper function fo checking wheter request is multipart/form-data request.
 *
 * @method     isFormData
 * @param      {Object}   req     Express Request object.
 * @return     {boolean}  true if request is multipart/form-data request.
 */
function isFormData(req) {
  var type = req.headers['content-type'] || '';
  return 0 === type.indexOf('multipart/form-data');
}

/**
 * Helper function for checking whether part is video file.
 *
 * @method     isVideoFile
 * @param      {Object}   part  Multiparty Part object.
 * @return     {boolean}  true if part has Content-Type header video/*
 */
function isVideoFile(part) {
  return 0 === part.headers['content-type'].indexOf('video/');
}

/**
 * Uploads video for transcoding.
 */
router.post('/',
  function (req, res, next) {
    if (!isFormData(req)) {
      return res.status(406).send({ message: 'Not Acceptable: expecting multipart/form-data' });
    }

    // parsing parameters
    var id = req.query.id || uuid.v4();

    var i;
    var formats = req.query.f ? req.query.f.split(',') : ['mp4'];
    for (i = 0; i < formats.length; i++) {
      if (presets.formats.indexOf(formats[i]) >= 0) {
        continue;
      }

      return res.status(400).send({ message: 'Bad Request: unknown format: ' + formats[i] });
    }
    var sizes = req.query.s ? req.query.s.split(',') : [720];
    sizes = sizes.map(function (s) { return parseInt(s); });
    for (i = 0; i < sizes.length; i++) {
      if (presets.sizes.indexOf(sizes[i]) >= 0) {
        continue;
      }

      return res.status(400).send({ message: 'Bad Request: unknown size: ' + sizes[i] });
    }


    // parsing request
    var form = new multiparty.Form({
      autoFields: true
    });

    var filesCount = 0;
    form.on('part', function (part) {
      if (filesCount > 0) {
        return;
      }

      if (!isVideoFile(part)) {
        return res.status(400).send({ message: 'Bad Request: expecting video/* file' });
      }

      // upload to S3
      filesCount++;
      var key = id + path.extname(part.filename);
      var uri = shivaUtils.getS3Uri(bucket, key);
      debug('Uploading video file ' + id + ' of size ' + part.byteCount + ' to ' + uri);

      var params = {
        Bucket: bucket,
        Key: key,
        ACL: 'public-read',
        Body: part,
        ContentLength: part.byteCount,
        ContentType: part.headers['content-type']
      };

      s3.putObject(params, function (err) {
        if (err) return next(err);

        debug("Video " + id + " successfully uploaded to " + uri);

        // building outputs
        var outputs = [];
        var i, j;
        for (i = 0; i < formats.length; i++) {
          for (j = 0; j < sizes.length; j++) {
            var fileName = id + '_' + sizes[j] + '.' + formats[i];
            var fileUri = shivaUtils.getS3Uri(bucket, fileName);
            outputs.push({
              uri: fileUri,
              format: formats[i],
              size: sizes[j]
            });
          }
        }

        // send event
        var event = {
          id: id,
          uri: uri,
          outputs: outputs
        };
        bus.publishVideoUploaded(event, function (err) {
          if (err) return next(err);

          var response = {
            id: id,
            outputs: outputs
          };
          res.json(response);
        });
      });
    });

    form.on('error', function (err) {
      return next(err);
    });

    form.parse(req);
  });

module.exports = router;
