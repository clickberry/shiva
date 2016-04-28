/**
 * @fileOverview Helper functions.
 */

 /**
 * Builds full S3 object URI.
 *
 * @method     getObjectUri
 * @param      {string}  bucket  S3 bucket name.
 * @param      {string}  key     Object key name.
 * @return     {string}  Full object URI.
 */
exports.getS3Uri = function(bucket, key) {
  return 'https://' + bucket + '.s3.amazonaws.com/' + key;
};