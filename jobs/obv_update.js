import {conn} from '../common/dbconn.js';

// 입력 시간으로부터 5분씩 증가하며 obv 업데이트
export const obvUpdate = async (startTime) => {
  console.log('obv update 시작');
  
  const fiveMinute = 60 * 5;
  let seq = 0;

  while(true){
    const time = startTime + (fiveMinute * seq++);
    const sql = make_obv_update_sql(time);
    
    // console.log(time + ' / ' + currTime);
    // console.log(sql);
    await conn.query(sql, (error, results, fields)=>{
      if(error){ console.log(error); }
      console.log(time ,' / ', results);
    });
    if(time > Math.floor(new Date().getTime() / 1000)) { 
      console.log(sql);
      console.log('obv 업데이트 종료');
      break; 
    }
  }
}

const make_obv_update_sql = (calc_timestamp) => {
  const sql = `
    UPDATE candles
    SET 
      obv_5 = FN_GET_OBV5(market, ${calc_timestamp}),
      obv_5_p10 = FN_GET_OBV_P10(market, ${calc_timestamp}, 5),
      obv_15 = FN_GET_OBV15(market, ${calc_timestamp}),
      obv_15_p10 = FN_GET_OBV_P10(market, ${calc_timestamp}, 15),
      obv_240 = FN_GET_OBV240(market, ${calc_timestamp}),
      obv_240_p10 = FN_GET_OBV_P10(market, ${calc_timestamp}, 240),
      editdate = now()
    WHERE calc_timestamp = ${calc_timestamp}
  `;
  return sql;
}

// doUpdate(1648566000); // 2022-03-30 00:00
