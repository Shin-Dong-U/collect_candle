import mysql from 'mysql';
import db_config from './db_properties.js';
import fetch from 'node-fetch';

export const conn = mysql.createConnection({ host: db_config.host, user: db_config.user, password: db_config.password, database: db_config.database });

const codes = await getKrwMarketCodes();

const getMinuteCandle = async (code, minuteUnit, lastTime, count) => {
  let collectionName = '';
  switch(minuteUnit){
    case 1: collectionName = 'minute_candles'; break;
    case 15: collectionName = 'minute_candles_15'; break;
    case 240: collectionName = 'minute_candles_240'; break;
  }

  const options = {method: 'GET', headers: {Accept: 'application/json'}};
  try {
    // 1개의 마켓에 대한 캔들 데이터 요청
    let response;
    let tryCount = 0;
    let isFail = false;
    while(true){
      response = await fetch(`https://api.upbit.com/v1/candles/minutes/${minuteUnit}?market=${code}&to=${lastTime}&count=${count}`, options);
      if(response.status === 200){ break; }
      
      if(tryCount++ > 10){
        console.log('업비트 요청한도 초과 - ' + code);
        isFail = true;
        break;
      }
    }
    
    let response_json;
    if(isFail){
      
    }else{
      response_json = await response.json();
    }
    
    Array.from(response_json).reverse().forEach(async (candle) => {
      candle = parseCandle(candle); // 데이터 정규화
      const calcTime = candle.calc_timestamp;
      
      // 캔들 데이터 저장
      const sql = await make_candle_insert_sql(candle);
      conn.query(sql);
    })

  } catch (error) {
    console.error('[' + code + '] ' + error); 
    // 재요청 
  }
}

const parseCandle = (candleData) => {
  candleData.calc_timestamp = parseTimestampToMinuteUnit(candleData.timestamp);
  
  candleData.obv_5 = 0;
  candleData.obv_5_p10 = 0;
  candleData.obv_15 = 0;
  candleData.obv_15_p10 = 0;
  candleData.obv_240 = 0;
  candleData.obv_240_p10 = 0;

  return candleData;
}

const parseTimestampToMinuteUnit = (timestamp) => {
  let secUnit = Math.floor(timestamp / 1000);
  let minuteUnit = secUnit - (secUnit % 60);
  return minuteUnit;
}

const getCurrMinuteCandle = () => { // 주기적으로 캔들 데이터 조회
  setInterval(()=>{
    const currTime = new Date().toISOString();
    executeMinuteCandle(currTime, codes, 1, 20);
  }, 1000 * 30);
}

// 기준시에서 1시간씩 증가시키며 캔들 데이터 조회 
const getPrevMinuteCandle = (criTime) => {
  console.log('start getPrevMinuteCandle');
  const hour = 1000 * 60 * 60;
  criTime = criTime - (criTime % (hour)) + 1000;
  let seq = 0;

  setInterval(()=>{
    const time = new Date(criTime + (hour * seq++)).toISOString();
    executeMinuteCandle(time, codes, 1, 62);
    console.log('ing getPrevMinuteCandle / ', time);
  }, 1000 * 3);
}

async function executeMinuteCandle(currTime, codes, timeUnit, count) {
  let i = 0;
  let len = codes.length;

  while (true) {
    await new Promise(resolve => setTimeout(resolve, 400));

    const code = codes[i++];
    // console.log(code);
    getMinuteCandle(code, timeUnit, currTime, count);
    
    if(i >= len) break;
  }
}

const makeCandlesDefaultData = async () => {
  await new Promise(resolve => setTimeout(resolve, 10000));
  for(let i = 0; i < codes.length; i++){
    const code = codes[i];
    const candle1 = new Candle({"market": code});
    const candle15 = new Candle15({"market": code});
    const candle240 = new Candle240({"market": code});
    candle1.save();
    candle15.save();
    candle240.save();
  }
}
// makeCandlesDefaultData();

//  getCurrMinuteCandle();
// let prevTime = 1648573200000; // 2022-03-30 02:00
const prevTime = 1648569600000; //  2022-03-30 01:00
getPrevMinuteCandle(prevTime);
// obvUpdateProcess(1648566300);

// let a = 1648569600 * 1000;
// getPrevMinuteCandle(a);


const make_candle_insert_sql = (candle) => { 
  let sql = `
      INSERT INTO candles(
        market,
        candle_date_time_utc,
        candle_date_time_kst,
        opening_price,
        high_price,
        low_price,
        trade_price,
        calc_timestamp,
        timestamp,
        candle_acc_trade_price,
        candle_acc_trade_volume,
        unit,
        obv_5,
        obv_5_p10,
        obv_15,
        obv_15_p10,
        obv_240,
        obv_240_p10
      ) values(
        '${candle.market}',
        '${candle.candle_date_time_kst}',
        '${candle.candle_date_time_utc}',
        ${candle.opening_price},
        ${candle.high_price},
        ${candle.low_price},
        ${candle.trade_price},
        ${candle.calc_timestamp},
        ${candle.timestamp},
        ${candle.candle_acc_trade_price},
        ${candle.candle_acc_trade_volume},
        ${candle.unit},
        FN_GET_OBV5('${candle.market}', ${candle.calc_timestamp}),
        FN_GET_OBV_P10('${candle.market}', ${candle.calc_timestamp}, 5),
        FN_GET_OBV15('${candle.market}', ${candle.calc_timestamp}),
        FN_GET_OBV_P10('${candle.market}', ${candle.calc_timestamp}, 15),
        FN_GET_OBV240('${candle.market}', ${candle.calc_timestamp}),
        FN_GET_OBV_P10('${candle.market}', ${candle.calc_timestamp}, 240)
      )
      ON DUPLICATE KEY
      UPDATE 
        candle_date_time_kst = '${candle.candle_date_time_kst}',
        candle_date_time_utc = '${candle.candle_date_time_utc}',
        opening_price = ${candle.opening_price},
        high_price = ${candle.high_price},
        low_price = ${candle.low_price},
        trade_price = ${candle.trade_price},
        timestamp = ${candle.timestamp},
        candle_acc_trade_price = ${candle.candle_acc_trade_price},
        candle_acc_trade_volume = ${candle.candle_acc_trade_volume},
        unit = ${candle.unit},
        obv_5 = FN_GET_OBV5('${candle.market}', ${candle.calc_timestamp}),
        obv_5_p10 = FN_GET_OBV_P10('${candle.market}', ${candle.calc_timestamp}, 5),
        obv_15 = FN_GET_OBV15('${candle.market}', ${candle.calc_timestamp}),
        obv_15_p10 = FN_GET_OBV_P10('${candle.market}', ${candle.calc_timestamp}, 15),
        obv_240 = FN_GET_OBV240('${candle.market}', ${candle.calc_timestamp}),
        obv_240_p10 = FN_GET_OBV_P10('${candle.market}', ${candle.calc_timestamp}, 240),
        editdate = now()
    `;
    // console.log(sql);
    return sql;
}

