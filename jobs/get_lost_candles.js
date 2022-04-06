import {conn} from '../common/dbconn.js';
import fetch from 'node-fetch';
// import {getCandlesFromUpbit} from './upbit/get_candles.js';

const doGet = async () => {

}

const findLostCandles = async () => {
  const CRITERIA_CALC_TIME = 1648566000; // 2022-03-30 00:00

  const sql = `
    SELECT market, calc_timestamp FROM candles 
    WHERE 
      calc_timestamp >= ${CRITERIA_CALC_TIME}
      AND (
        ( calc_timestamp % 300 = 0 AND obv_5 = 0  )
        OR ( calc_timestamp % 900 = 0 AND obv_15 = 0  )
        OR ( calc_timestamp % 14400 = 0 AND obv_240 = 0  )
      )
    ORDER BY calc_timestamp ASC
  `;
        console.log(sql);
  conn.query(sql, (error, results, fields) => {
    if(error){ console.log(error); }
    console.log(results);
    console.log(results.length);
  });
}

// findLostCandles();




