import assert from 'assert';
import db from '../common/dbconn.js';

const t1 = 'hello mocha';

// OBV DB 값 검증
const obvValueCheck = async (minuteUnit) => {
  const timeUnit = minuteUnit * 60;
  const limit = minuteUnit + 2;

  let beforeTime = Math.floor(new Date().getTime() / 1000);
  beforeTime = beforeTime - timeUnit - (beforeTime % (timeUnit));
  // let beforeTime = 1648569600; // 2022-03-30 01:00
  // if(minuteUnit == 240){
  //   beforeTime = 1648569600 + (240 * 60); 
  // }

  const sql = `
    SELECT *
    FROM candles 
    WHERE 
      market = 'KRW-BTC' 
      AND calc_timestamp <= ${beforeTime}
    ORDER BY calc_timestamp DESC
    LIMIT ${limit}
  `;
  // console.log(sql);

  const dbPool = await db.getPool(); 
  dbPool.getConnection((err, conn) => {
    if(err){ 
      if(conn) {conn.release();} 
      throw err; 
    }

    conn.query(sql, (error, results, fields)=>{
      if(error){ 
        console.log(error); 
        throw error;
      }

      let prevObv = 0;
      let prevClose = 0;
      let currClose = 0;
      let volume = 0;

      let expectedObv = 0;
      let actureObv = 0;

      for(let i = 0; i < results.length; i++){
        const row = results[i];
        // console.log(row);
        if(i === 0){
          expectedObv = row[`obv_${minuteUnit}`];
          continue;
        }else if(i === results.length -1){
          prevClose = row[`trade_price`];
          continue;
        }

        if(i === 1) currClose = row[`trade_price`];
        
        if(i === minuteUnit) prevObv = row[`obv_${minuteUnit}`];
        
        volume += row[`candle_acc_trade_volume`];
      }

      if(currClose === prevClose){
        volume = 0;
      }else if(currClose < prevClose){
        volume *= -1;
      }
      actureObv = prevObv + volume;

      console.log("기준시간 : " + beforeTime + " / 현재종가 : " + currClose, " / 이전종가 : " + prevClose + " / 이전OBV : " + prevObv + " / volume : " + volume + " / OBV계산값 : " + actureObv + " / OBV DB 값 : " + expectedObv);
      assert.equal(round(actureObv), round(expectedObv));
      if(conn) {conn.release();} 
    }); 
  });

}

const round = (val) => {
  return Math.round(val * 100000) / 100000;
}

describe('obv5 test', () => {
  obvValueCheck(5);
});
describe('obv15 test', () => {
  obvValueCheck(15);
});
describe('obv240 test', () => {
  obvValueCheck(240);
});