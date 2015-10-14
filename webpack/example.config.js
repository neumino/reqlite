var path = require('path');

module.exports = {
  devtool: 'eval-source-map',
  entry: {
    example: ['./example/index']
  },
  output: {
    path: path.join(__dirname, '../example'),
    filename: 'bundle.js',
    publicPath: '/example'
  },
  module: {
    loaders: [{
      test: /\.js$/,
      loaders: [],
      include: path.join(__dirname, '../example')
    }]
  },
  browser: {
    console: true,
    fs: 'empty',
    dgram: 'empty',
    dns: 'empty'
  }
};
