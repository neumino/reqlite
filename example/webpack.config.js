var path = require('path');

module.exports = {
  entry: {
    example: ['./example/index']
  },
  output: {
    path: path.join(__dirname),
    filename: 'bundle.js',
    publicPath: '/example'
  },
  module: {
    loaders: [{
      test: /\.js$/,
      loaders: ['babel?optional[]=runtime'],
      include: path.join(__dirname)
    }]
  }
};
