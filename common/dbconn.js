import mysql from 'mysql';
import db_config from '../config/db_properties.js';

/*
let conn;
export const getConnection = async () => {
  conn = await mysql.createConnection({ host: db_config.host, user: db_config.user, password: db_config.password, database: db_config.database });
  return conn;
}
*/

const settings = { host: db_config.host, user: db_config.user, password: db_config.password, database: db_config.database, connectionLimit: 5 };
export default  (() => {
  let dbPool;
  const initiate = async () => {
      return await mysql.createPool(settings)
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