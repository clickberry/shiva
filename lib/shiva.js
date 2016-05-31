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
var presets = require('./presets');
var url = require("url");

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
 * @param      {string}    filePath     Local file path.
 * @param      {string}    s3Bucket     S3 bucket name.
 * @param      {string}    s3Subdir     S3 sub-directory name.
 * @param      {string}    s3FileName   S3 file name.
 * @param      {string}    contentType  File content type.
 * @param      {Function}  fn           Callback.
 */
function uploadToS3(filePath, s3Bucket, s3Subdir, s3FileName, contentType, fn) {
  var fileStream = fs.createReadStream(filePath);
  var fileName = s3FileName || path.basename(filePath);
  var key = s3Subdir ? s3Subdir + '/' + fileName : fileName;
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
 * Lists S3 bucket objects by prefix.
 *
 * @param      {string}    s3Bucket  S3 bucket name.
 * @param      {string}    prefix    Objects prefix.
 * @param      {Function}  fn        Callback function.
 */
function listS3Objects(s3Bucket, prefix, fn) {
  var params = {
    Bucket: s3Bucket,
    Prefix: prefix
  };
  s3.listObjects(params, fn);
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
 * Concatenates video segments.
 *
 * @param      {string[]}  segments  Segment file paths.
 * @param      {Function}  fn        Callback function.
 */
function concatSegments(segments, fn) {
  debug('Concatenating video segments: ' + segments.join(', '));

  try {
    var proc = new ffmpeg(segments[0]);
    proc.then(function (video) {
      video.fnConcatSegments(segments, function (err, file) {
        if (err) return fn(err);
        fn(null, file);
      });
    }, function (err) {
      fn(err);
    });
  } catch (err) {
    fn(err);
  }
}

/**
 * Encodes video to MP4 format.
 *
 * @method     encodeToMp4
 * @param      {string}    videoPath  Video file path.
 * @param      {string}    format     Output format: {mp4,webm,gif}.
 * @param      {string}    size       Output size.
 * @param      {Function}  fn         Callback.
 */
function encodeVideo(videoPath, format, size, fn) {
  debug('Encoding ' + videoPath + ' to ' + format + ' ' + size + 'p');

  try {
    var proc = new ffmpeg(videoPath);
    proc.then(function (video) {
      var func;
      switch (format) {
        case 'mp4': func = 'fnEncodeToMp4'; break;
        case 'webm': func = 'fnEncodeToWebM'; break;
        case 'gif': func = 'fnEncodeToGif'; break;
        default: func = null;
      }
      if (!func) {
        return fn(new Error('Undefined format: ' + format));
      }

      video[func](size, function (err, file) {
        if (err) return fn(err);
        fn(null, file);
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
              uploadToS3(file, s3Bucket, s3Dir, null, contentType, function (err, uri) {
                if (err) return cb(err);

                var fileName = path.basename(uri);
                var pattern = /_(\d+)/;
                var match = pattern.exec(fileName);
                var idx = parseInt(match[1]);

                var event = {
                  uri: uri,
                  index: idx,
                  count: files.length
                };
                shiva.emit('segment', event);
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

/**
 * Downloads video and encodes it according to formats and sizes options.
 * Generates 'encoded' event when completed.
 *
 * @param      {string}    videoUri  URI of the video file to encode.
 * @param      {array}     formats   Video output formats.
 * @param      {array}     sizes     Video output sizes.
 * @param      {string}    s3Bucket  S3 bucket to upload encoded segments.
 * @param      {Function}  fn        Callback function.
 */
Shiva.prototype.encodeAndUploadVideoToS3 = function (videoUri, formats, sizes, s3Bucket, fn) {
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

  // create temp dir
  tmp.dir({unsafeCleanup: true}, function (err, tempPath, cleanupVideo) {
    if (err) return handleError(err);
  
    var fileName = path.basename(videoUri);
    var videoPath = path.join(tempPath, fileName);

    // download remote file
    downloadToLocal(videoUri, videoPath, function (err, contentType) {
      if (err) return handleError(err);

      var encodeTasks = [];
      formats.forEach(function (format) {
        sizes.forEach(function (size) {
          encodeTasks.push(function (cb) {
            encodeVideo(videoPath, format, size, cb);
          });
        });
      });

      // encoding serially
      async.series(encodeTasks,
        function (err, results) {
          if (err) return handleError(err);

          debug('Video encoding completed.');

          // building array of local outputs
          var localOutputs = [];
          var idx = 0;
          formats.forEach(function (f) {
            sizes.forEach(function (s) {
              localOutputs.push({
                format: f,
                size: s,
                file: results[idx++]
              });
            });
          });

          // uploading to S3
          var uploadTasks = [];
          var s3Path = url.parse(videoUri).pathname;
          var s3Dir = /\/([^\/]+)\//.exec(s3Path)[1];
          localOutputs.forEach(function (output) {
            uploadTasks.push(function (cb) {
              var contentType = presets.contentTypes[output.format];
              uploadToS3(output.file, s3Bucket, s3Dir, null, contentType, function (err, uri) {
                if (err) return cb(err);

                // delete local file
                fs.unlink(output.file, function (err) {
                  if (err) return cb(err);

                  delete output.file;
                  output.uri = uri;
                  cb(null, output);
                });
              });
            });
          });

          // upload parallel
          async.parallel(uploadTasks,
            function (err, results) {
              if (err) return handleError(err);

              debug('All encoded videos were successfully uploaded to S3.');

              // remove local files
              cleanupVideo();

              shiva.emit('encoded', results);
              fn(null, results);
            });
        });
    });
  });  
};

/**
 * Checkes whether all video segments have been processed.
 *
 * @param      {string}    segmentUri        Current segment URI.
 * @param      {string}    formatSizesCount  Number of formats * sizes for segment encoding.
 * @param      {string}    segmentsCount     Number of segments for video.
 * @param      {string}    s3Bucket          S3 bucket name.
 * @param      {Function}  fn                Callback function.
 */
Shiva.prototype.checkIfAllSegmentsProcessed = function (segmentUri, formatSizesCount, segmentsCount, s3Bucket, fn) {
  var s3Path = url.parse(segmentUri).pathname;
  var prefix = /^\/([^_]+)/.exec(s3Path)[1];
  listS3Objects(s3Bucket, prefix, function (err, data) {
    if (err) return fn(err);

    var keys = data.Contents.map(function (o) {
      return o.Key;
    });

    var segments = keys.filter(function (s) {
      return /_\d+_/.test(s);
    });

    if (segments.length !== segmentsCount * formatSizesCount) {
      return fn();
    }

    fn(null, segments);
  });
};

/**
 * Concatenates video segments and uploads result to the target S3 URL.
 *
 * @param      {array}    segments   Array of segment URIs.
 * @param      {string}    uri       Target S3 URI.
 * @param      {string}    format    Video format.
 * @param      {string}    s3Bucket  S3 bucket name.
 * @param      {Function}  fn        Callback function.
 */
Shiva.prototype.concatSegmentsToS3 = function (segments, uri, format, s3Bucket, fn) {
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

  // create temp dir
  tmp.dir({unsafeCleanup: true}, function (err, tempPath, cleanupTemp) {
    if (err) return handleError(err);
  
    var downloadTasks = [];
    segments.forEach(function (segment) {
      var fileName = path.basename(segment);
      var segmentPath = path.join(tempPath, fileName);

      // download segment
      (function (uri, localPath) {
        downloadTasks.push(function (cb) {
          downloadToLocal(uri, localPath, function (err) {
            if (err) return cb(err);
            cb(null, localPath);
          });
        });
      })(segment, segmentPath);
    });

    // downloading segments parrallel
    debug('Downloading ' + downloadTasks.length + ' video segments for cancatenation...');
    async.parallel(downloadTasks,
      function (err, results) {
        if (err) return handleError(err);

        debug('All video segments downloaded. Concatenating...');

        // sort segments
        var localSegments = results.sort(function(a, b){
          var aIdx = parseInt(/_(\d+)_/.exec(a)[1]);
          var bIdx = parseInt(/_(\d+)_/.exec(b)[1]);
          if(aIdx < bIdx) return -1;
          if(aIdx > bIdx) return 1;
          return 0;
        });

        concatSegments(localSegments, function (err, result) {
          if (err) return handleError(err);

          debug('Video segments concatenated successfully to file: ' + result);

          // uploading result to S3
          var s3FileName = path.basename(uri);
          var contentType = presets.contentTypes[format];
          uploadToS3(result, s3Bucket, null, s3FileName, contentType, function (err, uri) {
            if (err) return handleError(err);

            // delete local files
            cleanupTemp();

            fn(null, uri);
          });
        });
      });
  });
};

module.exports = Shiva;
