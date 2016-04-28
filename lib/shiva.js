/**
 * @fileOverview Encapsulates splitting, encoding and merging logic.
 */

var debug = require('debug')('clickberry:shiva:shiva');
var ffmpeg = require('ffmpeg');
var events = require('events');
var util = require('util');
var async = require('async');
var tmp = require('tmp');
var shivaUtils = require('./utils');
var path = require('path');
var fs = require('fs');
var request = require('request');
var uuid = require('node-uuid');
var AWS = require('aws-sdk');
var s3 = new AWS.S3();

/**
 * Helper function for downloading file from the URI to the local file path.
 *
 * @method     downloadFile
 * @param      {string}    uri       File URI.
 * @param      {string}    filePath  Local file path to download to.
 * @param      {Function}  fn        Callback.
 */
function downloadToLocal(uri, filePath, fn) {
  debug('Downloading ' + uri + ' to ' + filePath);

  var error;
  var contentType;
  var file = fs.createWriteStream(filePath)
    .on('finish', function() {
      if (error) {
        return this.close();
      }
      
      this.close(function (err) {
        if (err) return fn(err);
        debug('Downloading ' + uri + ' to file ' + filePath + ' completed.');
        fn(null, contentType);
      });
    });

  request
    .get(uri)
    .on('response', function(res) {
      if (200 != res.statusCode) {
        error = new Error('Invalid status code: ' + res.statusCode + ' while downloading ' + uri);
        error.fatal = true;
        return fn(error);
      }

      contentType = res.headers['content-type'];
    })
    .on('error', function(err) {
      debug('Downloading ' + uri + ' error: ' + err);
      fn(err);
    })
  .pipe(file);
}

/**
 * Helper function for uploading file to S3 bucket.
 *
 * @method     uploadFrameToS3
 * @param      {string}    s3Bucket     S3 bucket name.
 * @param      {string}    s3Subdir     S3 sub-directory name.
 * @param      {string}    filePath     local file path.
 * @param      {string}    contentType  File content type.
 * @param      {Function}  fn           Callback.
 */
function uploadToS3(s3Bucket, s3Subdir, filePath, contentType, fn) {
  var fileStream = fs.createReadStream(filePath);
  var fileName = path.basename(filePath);
  var key = s3Subdir + '/' + fileName;
  debug('Uploading file ' + filePath + ' to the bucket: ' + key);

  var params = {
    Bucket: s3Bucket,
    Key: key,
    ACL: 'public-read',
    Body: fileStream,
    ContentType: contentType
  };

  s3.upload(params, function (err) {
    if (err) return fn(err);

    var uri = shivaUtils.getS3Uri(s3Bucket, key);
    debug('File uploaded: ' + uri);
    fn(null, uri);
  });
}

/**
 * Extracts segments from the local video file to specified folder.
 *
 * @method     extractSegments
 * @param      {string}    videoPath  Video file path.
 * @param      {string}    format     Video format (extension).
 * @param      {string}    toDir      Target directory path.
 * @param      {Function}  fn         Callback.
 */
function extractSegments(videoPath, format, toDir, fn) {
  debug('Extracting segments from video ' + videoPath + ' to ' + toDir);

  var ext = path.extname(videoPath);
  var filePattern = path.join(toDir, path.basename(videoPath).replace(ext, '_%d.' + format));

  try {
    var proc = new ffmpeg(videoPath);
    proc.then(function (video) {
      video.fnExtractSegments(filePattern, function (err, files) {
        if (err) return fn(err);
        fn(null, files);
      });
    }, function (err) {
      fn(err);
    });
  } catch (err) {
    fn(err);
  }
}

/**
 * Helper function for breaking tasks array into batches.
 *
 * @method     breakIntoBatches
 * @param      {Array}   tasks      Array of tasks.
 * @param      {number}  batchSize  { description }
 * @return     {Array}   Array of arrays (batches) with length not bigger than
 *                       batchSize.
 */
function breakIntoBatches(tasks, batchSize) {
  batchSize = batchSize || 100;

  var batches = [];
  var batchCount = Math.floor(tasks.length / batchSize) + 1;
  var i;
  for (i = 0; i < batchCount; i++) {
    var start = i * batchSize;
    var end = Math.min((i + 1) * batchSize, tasks.length);
    var batchOfTasks = tasks.slice(start, end);
    batches.push(batchOfTasks);
  }

  return batches;
}

/**
 * Wraps array of tasks to execute them later parallelly.
 *
 * @method     processBatch
 * @param      {Array}     tasks   Array of tasks: function (callback) {}
 * @return     {Function}  Callback.
 */
function processBatch(tasks) {
  return function (fn) {
    debug('Executing batch...');
    
    async.parallel(tasks,
      function (err, results) {
        if (err) return fn(err);

        debug('Batch processed successfully.');
        fn(null, results);
      });
  };
}

/**
 * Shiva the encoding service.
 *
 * @class
 */
var Shiva = function (options) {
  options = options || {};

  /**
   * Additional options
   */
  this.options = options;
};

/**
 * Shiva inherits EventEmitter's on()/emit() methods.
 */
util.inherits(Shiva, events.EventEmitter);


/**
 * Downloads video and uploads segments to the S3 bucket. Generates 'segment'
 * event for each uploaded segment.
 *
 * @method     extractSegmentsToS3
 * @param      {string}    videoUri  Video URI to download and extract frames
 *                                   from.
 * @param      {string}    s3Bucket  S3 bucket name to upload segments to.
 * @param      {Function}  fn        Callback function.
 */
Shiva.prototype.extractSegmentsToS3 = function (videoUri, s3Bucket, fn) {
  var shiva = this;
  fn = fn || function (err) {
    if (err) debug(err);
  };

  var handleError = function (err) {
    // emit error event
    shiva.emit('error', err);

    // callback with error
    fn(err);
  };

  // create temp file
  tmp.file(function (err, videoPath, fd, cleanupVideo) {
    if (err) return handleError(err);

    // download remote file
    downloadToLocal(videoUri, videoPath, function (err, contentType) {
      if (err) return handleError(err);

      // create temp dir for frames
      tmp.dir({unsafeCleanup: true}, function (err, segmentsPath, cleanupSegments) {
        if (err) return handleError(err);

        // extract segments
        var format = path.extname(videoUri).substring(1);
        extractSegments(videoPath, format, segmentsPath, function (err, files) {
          // delete temp file
          cleanupVideo();
          if (err) return handleError(err);

          debug(files.length + ' segments extracted from video ' + videoPath);

          // creating tasks for uploading to S3 and generating 'segment' events
          var uploadTasks = [];
          var s3Dir = path.basename(videoUri, path.extname(videoUri));
          files.forEach(function (file) {
            uploadTasks.push(function (cb) {
              uploadToS3(s3Bucket, s3Dir, file, contentType, function (err, uri) {
                if (err) return handleError(err);
                shiva.emit('segment', uri);
                cb(null, uri);
              });
            });
          });

          // break tasks into the batches
          var batchArray = breakIntoBatches(uploadTasks, 10);
          var batches = [];
          batchArray.forEach(function (ts) {
            batches.push(processBatch(ts));
          });
          debug('Segments uploading is broken into ' + batches.length + ' batches');

          // execute batches serially
          async.series(batches,
            function (err, results) {
              if (err) return handleError(err);

              debug('All segments were successfully uploaded to S3.');

              // remove local files
              cleanupSegments();

              // emit end event
              var res = [];
              results.forEach(function (b) {
                b.forEach(function (f) {
                  res.push(f);
                });
              });
              shiva.emit('end', res);

              fn(null, res);
            });
        });
      });
    });
  });
};

module.exports = Shiva;
