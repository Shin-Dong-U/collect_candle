import fetch from 'node-fetch';
import {Candle} from './model/candle.js';
import getKrwMarketCodes from './marke_codes.js';

// obv 최초값 입력 기준 시 
const CRI_OBV5_CALC_TIME = 1648566000;
const CRI_OBV15_CALC_TIME = 1648566000;
const CRI_OBV240_CALC_TIME = 1648569600;

export const getObv = async (code, type, criTime) => {
  const len = type + 1;
  const prevObvIdx = len - 2;

  // 요청 시간이 기준시와 동일하거나 과거일 경우 0으로 처리
  if(type === 5 && criTime <= CRI_OBV5_CALC_TIME){ return 0; }
  if(type === 15 && criTime <= CRI_OBV15_CALC_TIME){ return 0; }
  if(type === 240 && criTime <= CRI_OBV240_CALC_TIME){ return 0; }

  // 1 criTime을 기준으로 이전 데이터 획득
  let candles = await Candle.aggregate([
    { $match: {market: code}},
      { $unwind: '$data'},
      { $match: {'data.calc_timestamp': {$lt: criTime}}},
      { $sort: {'data.calc_timestamp': -1}},
      { $limit: len}
  ]);
  
  // 2. 획득 데이터 검증 
  const isValid = validCandles(candles, type, criTime);
  
  // 3. 누락 데이터 처리 (작업 전)
  if(!isValid){

  }
  
  // print(candles);
  
  // 4. obv 계산
  const prevObv = candles[prevObvIdx].data['obv_' + type];
  const currObv = calcObv(candles, prevObv);
  // console.log("prevObv : ", prevObv, " / currObv : ", currObv);
  
  return currObv;
}

const validCandles = (candles, type, criTime) => {
  const len = candles.length;
  // 1. 배열 길이 검사
  if(type === 5){
    if(len !== 6) return false;
  }else if(type === 15){
    if(len !== 16) return false;
  }else if(type === 240){
    if(len !== 241) return false;
  }else{
    return false;
  }
  // 2. 데이터 시간 간격 검사
  const minute = 60;
  let seq = 1;
  candles.forEach(candle=>{
    if(candle.calc_time !== criTime - (minute * seq++)){
      return false;
    }
  })

  return true;
}

const calcObv = (candles, prevObv) => {
  const lastIdx = candles.length - 1;
  
  let volume = 0;
  for(let i = 0; i < candles.length; i++){
    if(i === lastIdx) break;
    volume += candles[i].data.candle_acc_trade_volume;
  }

  const prevCriCandle = candles[lastIdx].data;
  const prevClosePrice = prevCriCandle.trade_price;

  const currCandle = candles[0].data;
  const currClosePrice = currCandle.trade_price;

  if(currClosePrice == prevClosePrice){
    volume = 0;
  }else if(currClosePrice < prevClosePrice){
    volume *= -1;
  }

  return prevObv + volume;
}

export const getObvP10 = async (code, type, criTime) => {
  const len = type + 1;
  const prevObvIdx = len - 2;
  const colName = 'data.obv_' + type;
  // 1 criTime을 기준으로 이전 데이터 획득
  const candles = await Candle.aggregate([
    { $match: {market: code}},
      { $unwind: '$data'},
      { $match: {'data.calc_timestamp': {$lt: criTime}}},
      { $match: {[colName]: {$ne: 0}}},
      { $sort: {'data.calc_timestamp': -1}},
      { $limit: 10}
  ])
  // print(candles)
  let obv_p10 = 0;
  
  for(let i = 0; i < candles.length; i++){
    const candle = candles[i];
    const obv = candle.data['obv_' + type];
    obv_p10 += obv;
  }
  if(!obv_p10 || candles.length === 0) { return 0; }

  return obv_p10 / candles.length;
}

export const isNeedToCalcOBV5 = (calc_time) => {
  return calc_time % (60 * 5) === 0;
}

export const isNeedToCalcOBV15 = (calc_time) => {
  return calc_time % (60 * 15) === 0;
}

export const isNeedToCalcOBV240 = (calc_time) => { 
  return (calc_time) % (60 * 240) === 0;
}

const print = (candles) => {
  candles.forEach(candle=>{
    console.log(candle.data.calc_timestamp, ' / volume : ', candle.data.candle_acc_trade_volume, ' / price : ', candle.data.trade_price ,' / obv_5 : ', candle.data.obv_5,' / obv_15 : ', candle.data.obv_15, ' / obv_240 : ', candle.data.obv_240);
  });
}

// 모든 마켓의 obv값 업데이트 - 기준 시에서 1분씩 증가 -> 현재시간 도달 시 중지
export const obvUpdateProcess = async (criTime) => {
  console.log('start obvUpdateProcess');
  const codes = await getKrwMarketCodes();
  // console.log('in obvUpdateProccess codes : ', codes);
  // const criTime = 1648566000;
  const fiveMinute = 60 * 5;
  criTime = criTime - (criTime % fiveMinute);

  let seq = 1;
  while(true){
    const time = criTime + (fiveMinute * seq++);
    if(time >= Math.floor(new Date().getTime / 1000)){ break; }

    for(let i = 0; i < codes.length; i++){
      const code = codes[i];
      if(isNeedToCalcOBV5(time)){
        const obv_5 = await getObv(code, 5, time);
        const obv_5_p10 = await getObvP10(code, 5, time);

        if(code === "KRW-BTC") { console.log(code , 'obv_5 : ', obv_5, ' / obv_5_p10 : ', obv_5_p10); }
        await Candle.updateOne(
          { market: code, data: {$elemMatch:{calc_timestamp: {$eq: time}}}},
          {$set: {'data.$.obv_5': obv_5, 'data.$.obv_5_p10': obv_5_p10}}
        );
      }
      if(isNeedToCalcOBV15(time)){
        const obv15 = await getObv(code, 15, time);
        const obv15p10 = await getObvP10(code, 15, time);

        // if(code === "KRW-BTC") { console.log(code , 'obv_15 : ', obv15, ' / obv_15_p10 : ', obv15p10);}
        await Candle.updateOne(
          { market: code, data: {$elemMatch:{calc_timestamp: {$eq: time}}}},
          {$set: {'data.$.obv_15': obv15, 'data.$.obv_15_p10': obv15p10}}
        );
      }
      if(isNeedToCalcOBV240(time)){
        const obv240 = await getObv(code, 240, time);
        const obv240p10 = await getObvP10(code, 240, time);
        
        // if(code === "KRW-BTC") { console.log(code , 'obv_240 : ', obv240, ' / obv240p10 : ', obv240p10); }
        await Candle.updateOne(
          { market: code, data: {$elemMatch:{calc_timestamp: {$eq: time}}}},
          {$set: {'data.$.obv_240': obv240, 'data.$.obv_240_p10': obv240p10}}
        );
      }
    }
  }
  console.log('end obvUpdateProcess');
}