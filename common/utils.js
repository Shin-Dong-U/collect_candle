import db from './dbconn.js';

export const getPrevTimestamp = async (startTime) => {
  if(startTime){ return Number(startTime); }

  const ONE_HOUR = 1000 * 60 * 60;
  const lastTime = await lastestCandleInsertTime();

  return lastTime + ONE_HOUR;
}

const lastestCandleInsertTime = async () => {
  try{
    const pool = await db.getPool();
    const [rows] = await pool.query(`SELECT calc_timestamp FROM candles where market='KRW-BTC' ORDER BY calc_timestamp DESC LIMIT 1`);

    let calcTimestamp ;
    if(rows[0] && rows[0].calc_timestamp){
      calcTimestamp = rows[0].calc_timestamp;
    }else{
      calcTimestamp = getYesterdayTimestamp();
    }

    return calcTimestamp * 1000;
  }catch(error){
    console.log(error);
    return 0;
  }
}

// 어제 00시 타임스탬프 구하기
const getYesterdayTimestamp = () => {
  const ONE_DAY = 1000 * 60 * 60 * 24;
  
  const date = new Date();
  date.setDate(date.getDate() - 1);
  let timestamp = date.getTime();

  return timestamp - (timestamp % ONE_DAY);
}
