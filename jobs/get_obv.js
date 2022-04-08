import fetch from 'node-fetch';
import getKrwMarketCodes from '../upbit/marke_codes.js';
import db from '../common/dbconn.js';

const codes = await getKrwMarketCodes();

const failArr = new Array();

/* 분캔들 요청
  param 
    code : String 마켓코드명
    miniteUnit: int 분캔들 요청단위(1분 캔들 요청시 -> 1입력 )
    timestamp long 요청시간
    timeISO : String 요청시간(ISO 형식)
    count : int 요청 캔들 갯수(최대 200개)
*/
const getMinuteCandle = async (code, minuteUnit, timestamp, timeISO, count) => {
  if(timestamp > new Date().getTime()){ return; } // 요청 시간이 현재 시간보다 크다면 리턴
  
  let reqTimestamp10 = Math.floor(timestamp / 1000);
  reqTimestamp10 = reqTimestamp10 - (reqTimestamp10 % 60);

  // 1개의 마켓에 대한 캔들 데이터 요청
  const options = {method: 'GET', headers: {Accept: 'application/json'}};
  const response = await fetch(`https://api.upbit.com/v1/candles/minutes/${minuteUnit}?market=${code}&to=${timeISO}&count=${count}`, options);
  if(response.status != 200){ // 데이터 획득에 실패
    console.log(`[${code}] status: ${response.status} - ${response.statusText}`);

    const failOrder = {'code': code, 'minuteUnit': minuteUnit, 'timestamp': timestamp,'timeISO': timeISO, 'count': count, 'status': response.status, 'statusText': response.statusText};
    failArr.push(failOrder); // 실패 목록에 추가

    return;
  }

  let response_json;
  response_json = await response.json();
  const taskArr = Array.from(response_json).reverse(); // 순차적 삽입을 위한 역순 정렬
  
  const dbPool = await db.getPool(); 
  dbPool.getConnection((err, conn) => { // 요청 갯수만큼 1개의 커넥션으로 처리
    if(err){ 
      if(conn) {conn.release();} 
      throw err; 
    }
    
    // 오래된 시간 부터 1분씩 증가시키며 삽입 할 데이터가 있는지 확인 -> 없다면 직전 데이터를 기준으로 데이터 가공 후 삽입
    const oldestTimestamp10 = reqTimestamp10 - ( 60 * (count-1) );
    
    let seq = 0;
    let seqTimestamp;
    for(let i = 0; i < count; i++){
      seqTimestamp = oldestTimestamp10 + (60 * seq);
      let candle = taskArr[0];
      if(candle){ candle = parseCandle(candle); }
      
      if(candle && candle.calc_timestamp < oldestTimestamp10){ // 예상 시간대를 벗어나는 데이터는 무시
        taskArr.shift(); // 해당 데이터 제거
        i--; // 이 경우 루프 카운트 X
        continue;
      }

      let sql = '';
      if(!candle || candle.calc_timestamp > seqTimestamp){ // 응답받은 분캔들에 비어있는 시간대가 존재하면 직전 데이터를 기준으로 데이터 생성 후 삽입
        // 더미 데이터 생성
        sql = make_sql_dummy_candle_insert(code, seqTimestamp);
      }else{ // 정상 데이터 
        sql = make_sql_candle_insert(candle);
        taskArr.shift();
      }
      
      conn.query(sql, (error, results, fields)=>{
        if(error){ console.log(error); }
      });
      seq++;
    }

    if(conn) {conn.release();}
  });
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

// 분캔들 조회시 *시 *분 01초가 되는 시점에 이후 프로세스를 수행하기 위하여 대기시간을 계산하는 함수
const calcWattingTime = () => {
  let startDelay = 0;
  
  const currSeconds = new Date().getSeconds();
  if(currSeconds > 1){ 
    startDelay = (61 - currSeconds) * 1000; 
  }else if(currSeconds < 1){ 
    startDelay = 1000; 
  }
  return startDelay;
}

// 1분간격으로 분 캔들 획득
export const getCurrMinuteCandle = async () => { 
  // *시 *분 01초까지 대기 후 시작
  const startDelay = calcWattingTime();
  await new Promise(resolve => setTimeout(resolve, startDelay));

  const DEFAULT_CANDLE_COUNT = 1;
  const FIRST_REQ_CANDLE_COUNT = 200;

  const DEFAULT_INTERVAL = 120;
  const FIRST_REQ_INTERVAL = 400;
  
  let loopCount = 0;
  setInterval(()=>{
    const timestamp = new Date().getTime();
    const reqTimeISO = new Date(timestamp).toISOString();

    const count = (loopCount++ == 0) ? FIRST_REQ_CANDLE_COUNT : DEFAULT_CANDLE_COUNT;
    const interval = (loopCount++ == 0) ? FIRST_REQ_INTERVAL : DEFAULT_INTERVAL;
    executeMinuteCandle(timestamp, reqTimeISO, codes, 1, count);
  }, 1000 * 60);
}

// 기준시에서 3시간씩 증가시키며 캔들 데이터 조회 
export const getPrevMinuteCandle = (startTime, afterFunc) => {
  console.log('start getPrevMinuteCandle');
  const hour = 1000 * 60 * 60;
  let seq = 0;

  const interval = setInterval(()=>{
    const reqTimestamp = startTime + (hour * 3 * seq++); // 기준시에서 n 시간 증가한 값
    const reqTimeISO = new Date(reqTimestamp).toISOString(); 

    executeMinuteCandle(reqTimestamp, reqTimeISO, codes, 1, 200, 200);
    console.log('ing getPrevMinuteCandle / ', reqTimeISO);

    if(reqTimestamp > new Date().getTime()){ 
      console.log('end getPrevMinuteCandle / ', reqTimeISO);
      clearInterval(interval); 
      if(afterFunc){ afterFunc(); }
    }
  }, 1000 * 25);
}

const executeMinuteCandle = async (timestamp, timeISO, codes, timeUnit, count, interval) => {
  if(!interval){ interval = 400; }

  let i = 0;
  let len = codes.length;

  while (true) {
    await new Promise(resolve => setTimeout(resolve, interval));

    const code = codes[i++];
    // console.log(code);
    getMinuteCandle(code, timeUnit, timestamp, timeISO, count);
    
    if(i >= len) break;
  }
}

const make_sql_candle_insert = (candle) => {
  let utc = candle.candle_date_time_utc.substring(0, 19);
  let kst = candle.candle_date_time_kst.substring(0, 19);
  
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
        '${utc}',
        '${kst}',
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
        candle_date_time_utc = '${utc}',
        candle_date_time_kst = '${kst}',
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

// 데이터 누락 시 이전 레코드로 더미 데이터 생성 쿼리
const make_sql_dummy_candle_insert = (market, calc_timestamp) => {
  let timestamp = calc_timestamp * 1000;
  let utc = new Date(timestamp).toISOString().substring(0, 19);
  let kst = new Date( timestamp + (1000 * 60 * 60 * 9) ).toISOString().substring(0, 19);
  
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
    ) 
    SELECT
      market,
      '${utc}',
      '${kst}',
      opening_price,
      high_price,
      low_price,
      trade_price,
      ${calc_timestamp},
      ${timestamp},
      0,
      0,
      unit,
      FN_GET_OBV5('${market}', ${calc_timestamp}),
      FN_GET_OBV_P10('${market}', ${calc_timestamp}, 5),
      FN_GET_OBV15('${market}', ${calc_timestamp}),
      FN_GET_OBV_P10('${market}', ${calc_timestamp}, 15),
      FN_GET_OBV240('${market}', ${calc_timestamp}),
      FN_GET_OBV_P10('${market}', ${calc_timestamp}, 240)
    FROM candles t
    WHERE 
      market = '${market}'
      AND calc_timestamp <= ${calc_timestamp}
    LIMIT 1

    ON DUPLICATE KEY 
    UPDATE
      candle_date_time_utc = '${utc}',
      candle_date_time_kst = '${kst}',
      opening_price = t.opening_price,
      high_price = t.high_price,
      low_price = t.low_price,
      trade_price = t.trade_price,
      timestamp = ${timestamp},
      candle_acc_trade_price = 0,
      candle_acc_trade_volume = 0,
      unit = t.unit,
      obv_5 = FN_GET_OBV5('${market}', ${calc_timestamp}),
      obv_5_p10 = FN_GET_OBV_P10('${market}', ${calc_timestamp}, 5),
      obv_15 = FN_GET_OBV15('${market}', ${calc_timestamp}),
      obv_15_p10 = FN_GET_OBV_P10('${market}', ${calc_timestamp}, 15),
      obv_240 = FN_GET_OBV240('${market}', ${calc_timestamp}),
      obv_240_p10 = FN_GET_OBV_P10('${market}', ${calc_timestamp}, 240),
      editdate = now()      
  `;
  // console.log(sql);
  return sql;
}

export const retryFailCandle = async () => { // 0.5 초마다 실패한 요청이 있는지 확인 후 있다면 재 요청
  setInterval(()=>{
    if(failArr.length > 0){
      const fail = failArr.shift();
      console.log('fail : ', fail);
      getMinuteCandle(fail.code, fail.minuteUnit, fail.timestamp, fail.timeISO, fail.count);
    }
  }, 500)
}

