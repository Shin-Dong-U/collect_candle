import mysql from 'mysql';
import db_config from '../config/db_properties.js';

export const conn = mysql.createConnection({ host: db_config.host, user: db_config.user, password: db_config.password, database: db_config.database });