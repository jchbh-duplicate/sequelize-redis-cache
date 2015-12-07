var Promise = require('bluebird');
var CircularJSON = require('circular-json');
var crypto = require('crypto');
var redis = null;
var sequelize = null;

module.exports = init;

/**
 * Initializer to return the cacher constructor
 */
function init(seq, red) {
  sequelize = seq;
  redis = red;
  return Cacher;
}

/**
 * Constructor for cacher
 */
function Cacher(model) {
  if (!(this instanceof Cacher)) {
    return new Cacher(model);
  }
  this.modelName = model;
  this.model = sequelize.model(model);
  this.seconds = 0;
  this.cacheHit = false;
  this.cachePrefix = 'cacher';
}

/**
 * Set cache prefix
 */
Cacher.prototype.prefix = function prefix(cachePrefix) {
  this.cachePrefix = cachePrefix;
  return this;
};

/**
 * Set redis TTL (in seconds)
 */
Cacher.prototype.ttl = function ttl(seconds) {
  this.seconds = seconds;
  return this;
};

/**
 * Fetch from the database
 */
Cacher.prototype.fetchFromDatabase = function fetchFromDatabase(key, queryMethod, opts) {
  var method = this.model[queryMethod];
  var self = this;
  return new Promise(function promiser(resolve, reject) {
    if (!method) {
      return reject(new Error('Invalid method - ' + queryMethod));
    }
    return method.apply(self.model, opts)
      .then(function then(results) {
        var res;
        if (!results) {
          res = results;
        } else if (Array.isArray(results)) {
          res = results;
        } else if (results.toString() === '[object SequelizeInstance]') {
          res = results.get({ plain: true });
        } else {
          res = results;
        }
        return self.setCache(key, res, self.seconds)
          .then(
            function good() {
              return resolve(res);
            },
            function bad(err) {
              return reject(err);
            }
          );
      },
      function(err) {
        reject(err);
      });
  });
};

/**
 * Set data in cache
 */
Cacher.prototype.setCache = function setCache(key, results, ttl) {
  return new Promise(function promiser(resolve, reject) {
    var res;
    try {
      res = JSON.stringify(results);
    } catch (e) {
      return reject(e);
    }
    return redis.setex(key, ttl, res, function(err, res) {
      if (err) {
        return reject(err);
      }
      return resolve(res);
    });
  });
};

/**
 * Clear cache with given query
 */
Cacher.prototype.clearCache = function clearCache(queryMethod, opts) {
  var self = this;
  if(queryMethod === undefined && opts === undefined)
      return this.clearAllCache();
  return new Promise(function promiser(resolve, reject) {
    var key = self.key(queryMethod, opts);
    return redis.del(key, function onDel(err) {
      if (err) {
        return reject(err);
      }
      return resolve();
    });
  });
};

Cacher.prototype.clearAllCache = function(){
    return new Promise(function(resolve, reject){
        return redis.keys(this.cachePrefix + ':' + this.modelName + '*', function(keys){
            return redis.del(keys, function(err){
                if(err) return reject(err);
                return resolve();
            })
        })
    })
}

/**
 * Fetch data from cache
 */
Cacher.prototype.fetchFromCache = function fetchFromCache(queryMethod, opts) {
  var self = this;
  return new Promise(function promiser(resolve, reject) {
    var key = self.key(queryMethod, opts);
    return redis.get(key, function(err, res) {
      if (err) {
        return reject(err);
      }
      if (!res) {
        return self.fetchFromDatabase(key, queryMethod, opts).then(resolve, reject);
      }
      self.cacheHit = true;
      try {
        return resolve(JSON.parse(res));
      } catch (e) {
        return reject(e);
      }
    });
  });
};

/**
 * Execute the query and return a promise
 */
Cacher.prototype.query = Cacher.prototype.fetchFromCache;

/**
 * Create redis key
 */
Cacher.prototype.key = function key(queryMethod, options) {
    console.log(options, CircularJSON.stringify(options, jsonReplacer));
  var hash = crypto.createHash('sha1')
    .update(CircularJSON.stringify(options, jsonReplacer))
    .digest('hex');
  return [this.cachePrefix, this.modelName, queryMethod, hash].join(':');
};

/**
 * Duck type to check if this is a sequelize DAOFactory
 */
function jsonReplacer(key, value) {
  if (value && value.DAO && value.sequelize) {
    return value.name;
  }else if(value instanceof Error){
    return "[Error]";
  }else if(value instanceof Promise){
    return "[Promise]";
  }
  return value;
}

/**
 * Add a retrieval method
 */
function addMethod(key) {
  Cacher.prototype[key] = function () {
    return this.query(key, Array.prototype.slice.call(arguments));
  };
}

var methods = [
  'find',
  'findOne',
  'findAll',
  'findAndCountAll',
  'all',
  'min',
  'max',
  'sum',
  'count'
];

methods.forEach(addMethod);
