/* eslint-disable no-param-reassign */
const loClone = require('lodash/clone');
const loForEach = require('lodash/forEach');
const loIsEmpty = require('lodash/isEmpty');
const { ObjectID } = require('mongodb');
const { MongoClient } = require('mongodb');

class MongoHandler {
  /**
   * @param config { db: '', url: '' }
   */
  constructor(config) {
    this.config = config;
    this.connection = {};
    this.client = undefined;
  }

  /**
   * @returns {Promise<*>}
   */
  async openConnection(db) {
    const database = db || this.config.db;
    if (this.connection[database]) {
      return this.connection[database];
    }
    const client = await this.getClient();
    this.connection[database] = client.db(database);
    return this.connection[database];
  }

  getClient() {
    if (this.client) {
      return this.client;
    }
    return MongoClient.connect(this.config.url, { useNewUrlParser: true, useUnifiedTopology: true })
      .then((client) => {
        this.client = client;
        return this.client;
      });
  }

  /**
   * @returns {Promise<*>}
   */
  async closeConnection() {
    const conn = this.client;
    if (!conn) {
      return true;
    }
    return conn.close()
      .then(() => {
        this.connection = {};
        this.client = undefined;
        return true;
      });
  }

  static debug(...message) {
    console.log(...message);
  }

  static convertKey(id) {
    if (Array.isArray(id)) {
      id = id.map((i) => new ObjectID(i));
    } else if (typeof id === 'string' && id.length === 24) {
      try {
        id = new ObjectID(id);
      } catch (e) {
        MongoHandler.debug(`${id} is not valid objectId`);
      }
    }
    return id;
  }

  /**
   *
   * @param select [] | * | ""
   * @returns {*}
   */
  static getSelectFields(select = '*') {
    let selected;
    // check fields
    if (Array.isArray(select)) {
      selected = {};
      select.forEach((s) => {
        selected[s] = 1;
      });
    } else if (loIsEmpty(select) || select === '*') {
      // default value
      selected = undefined;
    } else {
      selected = { [select]: 1 };
    }
    return selected;
  }

  /**
   *
   * @param order
   * @returns {*}
   */
  static getOrderByFields(order = []) {
    if (!order || order.length <= 0) {
      return {};
    }
    const orderBy = {};
    // order
    if (Array.isArray(order) === true) {
      order.forEach((o) => {
        if (o.indexOf('-') === 0) {
          orderBy[o.substr(1)] = -1;
        } else {
          orderBy[o] = 1;
        }
      });
    } else if (typeof order === 'object') {
      loForEach(order, (value, key) => {
        if (value.toLowerCase() === 'desc') {
          orderBy[key] = -1;
        } else {
          orderBy[key] = 1;
        }
      });
    } else {
      orderBy[order] = 1;
    }

    return orderBy;
  }

  static isIdField(field) {
    return (field === 'id' || field.indexOf('_id') !== -1);
  }

  static fixIdField(field) {
    return field === 'id' ? '_id' : field;
  }

  conditionBuilder(conditions = []) {
    let opr;
    let condition;
    let temp;
    let isFirst;
    let compiled;
    let where;
    const lookups = [];
    const addFields = [];

    isFirst = true;
    const sampleCondition = {
      field: '',
      operator: '=',
      value: '',
      condition: '$and',
    };
    const operators = {
      '=': '$eq',
      '<': '$lt',
      '>': '$gt',
      '<=': '$lte',
      '>=': '$gte',
      '<>': '$ne',
      '!=': '$ne',
      // 'like': ,
      // 'not like',
      // 'ilike',
      regexp: '$regex',
      between: 'between',
      in: '$in',
      'not in': '$nin',
    };

    compiled = {};
    loForEach(conditions, (cond, key) => {
      // for key-value pairs
      if (typeof cond !== 'object' || cond === null) {
        temp = cond;
        cond = loClone(sampleCondition);
        cond.field = key;
        cond.value = temp;
      }

      // Operator
      opr = '=';
      if (cond.operator && operators[cond.operator]) {
        opr = cond.operator;
      }
      opr = operators[opr];
      // condition
      condition = '$and';
      if (cond.condition && !loIsEmpty(cond.condition)) {
        condition = cond.condition.toLocaleLowerCase() === 'or' ? '$or' : '$and';
      }

      if (MongoHandler.isIdField(cond.field)) {
        cond.value = MongoHandler.convertKey(cond.value);
        cond.field = MongoHandler.fixIdField(cond.field);
      }

      where = { [cond.field]: {} };
      switch (opr) {
        case 'between':
          where[cond.field] = {
            $gte: cond.value[0],
            $lte: cond.value[1],
          };
          break;
        case '$regex':
          where[cond.field] = {
            [opr]: new RegExp(cond.value),
          };
          break;
        case '$eq':
          // for joins (very specific use case)
          if (cond.field.indexOf('$') === 0 && cond.value.indexOf('$') === 0) {
            where = { $eq: [cond.field, cond.value] };
          } else {
            where[cond.field] = cond.value;
          }
          break;
        case '$in':
        case '$nin':
          if (!Array.isArray(cond.value)) {
            cond.value = [cond.value];
          }
        // no break
        // eslint-disable-next-line no-fallthrough
        default:
          where[cond.field][opr] = cond.value;
      }
      if (isFirst === true) {
        compiled = where;
      } else {
        compiled = {
          [condition]: [where, compiled],
        };
      }
      isFirst = false;
    });

    let final = [];

    if (lookups.length) {
      final = final.concat(lookups);
    }

    if (!loIsEmpty(compiled)) {
      final.push({ $match: compiled });
    }

    if (!loIsEmpty(addFields)) {
      final = final.concat(addFields);
    }

    return final;
  }

  async query({ table, condition, select, order, from = 0, limit = 100 }) {
    condition = await this.conditionBuilder(condition);
    select = MongoHandler.getSelectFields(select);
    order = MongoHandler.getOrderByFields(order);
    let query = [];
    if (!loIsEmpty(condition)) {
      query = query.concat(condition);
    }

    if (select) {
      query.push({ $project: select });
    }

    if (!loIsEmpty(order)) {
      query.push({ $sort: order });
    }

    if (from) {
      query.push({ $skip: from });
    }

    if (limit) {
      query.push({ $limit: limit });
    }

    MongoHandler.debug(JSON.stringify(query));
    const connection = await this.openConnection();
    return connection.collection(table).aggregate(query).toArray();
  }
}

module.exports = MongoHandler;
