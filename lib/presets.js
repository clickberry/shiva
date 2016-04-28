/**
 * @fileOverview Standard video parameters.
 */

/**
 * Output formats.
 */
exports.formats = ['mp4', 'webm', 'gif'];
exports.defaultFormat = 'mp4';


/**
 * Format content types.
 */

exports.contentTypes = {
  'mp4': 'video/mp4',
  'webm': 'video/webm',
  'gif': 'image/gif'
};

/**
 * Frame sizes.
 */
exports.sizes = [
  360, 
  480, 
  720,  // HD
  1080, // Full HD
  2160, // 4K
  4320  // 8K
];
exports.defaultSize = '720';