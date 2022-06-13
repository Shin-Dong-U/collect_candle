import mysql from 'mysql2/promise';
import db_config from '../config/db_properties.js';

export default  (() => {
  let dbPool;
  const initiate = async () => {
      return mysql.createPool(db_config);
  }
  return {
    getPool: async function () {
        if (!dbPool) {
            dbPool = await initiate();
        }
        return dbPool;
    }
  }
})();