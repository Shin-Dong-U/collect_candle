import mongoose from 'mongoose';
import fetch from 'node-fetch';

const codes = [
  "KRW-BTC"
 ,"KRW-ETH"
 ,"KRW-NEO"
 ,"KRW-MTL"
 ,"KRW-LTC"
 ,"KRW-XRP"
 ,"KRW-ETC"
 ,"KRW-OMG"
 ,"KRW-SNT"
 ,"KRW-WAVES"
 ,"KRW-XEM"
 ,"KRW-QTUM"
 ,"KRW-LSK"
 ,"KRW-STEEM"
 ,"KRW-XLM"
 ,"KRW-ARDR"
 ,"KRW-ARK"
 ,"KRW-STORJ"
 ,"KRW-GRS"
 ,"KRW-REP"
 ,"KRW-ADA"
 ,"KRW-SBD"
 ,"KRW-POWR"
 ,"KRW-BTG"
 ,"KRW-ICX"
 ,"KRW-EOS"
 ,"KRW-TRX"
 ,"KRW-SC"
 ,"KRW-ONT"
 ,"KRW-ZIL"
 ,"KRW-POLY"
 ,"KRW-ZRX"
 ,"KRW-LOOM"
 ,"KRW-BCH"
 ,"KRW-BAT"
 ,"KRW-IOST"
 ,"KRW-RFR"
 ,"KRW-CVC"
 ,"KRW-IQ"
 ,"KRW-IOTA"
 ,"KRW-MFT"
 ,"KRW-ONG"
 ,"KRW-GAS"
 ,"KRW-UPP"
 ,"KRW-ELF"
 ,"KRW-KNC"
 ,"KRW-BSV"
 ,"KRW-THETA"
 ,"KRW-QKC"
 ,"KRW-BTT"
 ,"KRW-MOC"
 ,"KRW-ENJ"
 ,"KRW-TFUEL"
 ,"KRW-MANA"
 ,"KRW-ANKR"
 ,"KRW-AERGO"
 ,"KRW-ATOM"
 ,"KRW-TT"
 ,"KRW-CRE"
 ,"KRW-MBL"
 ,"KRW-WAXP"
 ,"KRW-HBAR"
 ,"KRW-MED"
 ,"KRW-MLK"
 ,"KRW-STPT"
 ,"KRW-ORBS"
 ,"KRW-VET"
 ,"KRW-CHZ"
 ,"KRW-STMX"
 ,"KRW-DKA"
 ,"KRW-HIVE"
 ,"KRW-KAVA"
 ,"KRW-AHT"
 ,"KRW-LINK"
 ,"KRW-XTZ"
 ,"KRW-BORA"
 ,"KRW-JST"
 ,"KRW-CRO"
 ,"KRW-TON"
 ,"KRW-SXP"
 ,"KRW-HUNT"
 ,"KRW-PLA"
 ,"KRW-DOT"
 ,"KRW-SRM"
 ,"KRW-MVL"
 ,"KRW-STRAX"
 ,"KRW-AQT"
 ,"KRW-GLM"
 ,"KRW-SSX"
 ,"KRW-META"
 ,"KRW-FCT2"
 ,"KRW-CBK"
 ,"KRW-SAND"
 ,"KRW-HUM"
 ,"KRW-DOGE"
 ,"KRW-STRK"
 ,"KRW-PUNDIX"
 ,"KRW-FLOW"
 ,"KRW-DAWN"
 ,"KRW-AXS"
 ,"KRW-STX"
 ,"KRW-XEC"
 ,"KRW-SOL"
 ,"KRW-MATIC"
 ,"KRW-NU"
 ,"KRW-AAVE"
 ,"KRW-1INCH"
 ,"KRW-ALGO"
 ,"KRW-NEAR"
 ,"KRW-WEMIX"
 ,"KRW-AVAX"
 ,"KRW-CELO"
 ,"KRW-T"
 ];

// const codes = [];
// const getKrwMarketCodes = async () => {
//   // if(codes !== []){ return;}
//   const options = {method: 'GET', headers: {Accept: 'application/json'}};
//   const response = await (await fetch('https://api.upbit.com/v1/market/all?isDetails=false', options)).json();

//   const codes = [];
//   Array.from(response).filter(e => e.market.startsWith("KRW-")).forEach((e)=> {
//     codes.push(e.market);
//   });

//   return codes;
// }

const connUrl = 'mongodb://192.168.219.106:27017/crypto';
mongoose.connect(connUrl);

var db = mongoose.connection;

db.once("open", function() {
  console.log("MongoDB database connection established successfully");
});

const candleSchema = new mongoose.Schema({
  market: String,
  data:[
    {
      market: String,
      candle_date_time_utc: String,
      candle_date_time_kst: String,
      opening_price: Number,
      high_price: Number,
      low_price: Number,
      trade_price: Number,
      calc_timestamp: Number,
      timestamp: Number,
      candle_acc_trade_price: Number,
      candle_acc_trade_volume: Number,
      unit: Number,
      obv_5: Number,
      obv_5_p10: Number,
      obv_15: Number,
      obv_15_p10: Number,
      obv_240: Number,
      obv_240_p10: Number
    }
  ]
})
// candleSchema.index({market: 1, calc_timestamp: 1}, {unique: true});

const Candle = mongoose.model('minute_candle', candleSchema, 'minute_candles');
const Candle15 = mongoose.model('minute_candle_15', candleSchema, 'minute_candles_15');
const Candle240 = mongoose.model('minute_candle_240', candleSchema, 'minute_candles_240');

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
    while(true){
      response = await fetch(`https://api.upbit.com/v1/candles/minutes/${minuteUnit}?market=${code}&to=${lastTime}&count=${count}`, options);
      if(response.status === 200){ break; }
      
      if(tryCount++ > 10){
        throw new Error('업비트 요청한도 초과');
        // 에러로그 기록 혹은 재시도 로직 
        // 실패 시 직전 데이터 카피
      }
    }
    


    let response_json = await response.json();
    
    // Todo. 코드 개선필요
    // 현재 프로세스 응답 데이터 배열을 1. 각각 데이터 존재하는지 조회 하고 2. 각각 삽입 DB 커넥션이 불필요하게 많이 일어 남
    Array.from(response_json).forEach(async (candle) => {
      candle = parseCandle(candle); // 데이터 정규화
      const calcTime = candle.calc_timestamp;
      
      /*
      if(isNeedToCalcOBV5(calcTime)){
        candleData.obv_5 = 계산
        candleData.obv_5_p10 = 계산
      }

      if(isNeedToCalcOBV15(calcTime)){
        candleData.obv_15 = 계산
        candleData.obv_15_p10 = 계산
      }

      if(isNeedToCalcOBV240(calcTime)){
        candleData.obv_240 = 계산
        candleData.obv_240_p10 = 계산
      }
      */

      await db.collection(collectionName).find({"market": code, "data": {"$elemMatch":{"calc_timestamp": candle.calc_timestamp}}}).count().then(async (cnt) => {
        if(cnt === 0){ // 해당 시간의 데이터가 존재하지 않으면 데이터 삽입
          await db.collection(collectionName).findOneAndUpdate({"market": code}, {$push: {data: candle}}, {upsert:true});
        }
      });
    })
  } catch (error) {
    console.error('[' + code + '] ' + error); 
    // 재요청 
  }
  
    // catch(err => {);
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

const getCurrMinuteCandle = () => { // 1분마다 캔들 데이터 조회
  setInterval(()=>{
    const currTime = new Date().getTime().getISOString();
    executeMinuteCandle(currTime, codes, 1, 5);
    // executeMinuteCandle(currTime, codes, 15, 10);
    // executeMinuteCandle(currTime, codes, 240, 10);
  }, 60000);
}

const getPrevMinuteCandle = () => { // 1분마다 30분전의 캔들 데이터 조회 ex) 첫번재 실행 시 0분전, 두번째 실행 시 30분 전
  const criTimestamp = new Date().getTime();
  const halfHour = 1000 * 60 * 30;
  let seq = 0;

  setInterval(()=>{
    const prevTime = new Date(criTimestamp - (halfHour * seq++)).toISOString();
    executeMinuteCandle(prevTime, codes, 1, 100);
    // executeMinuteCandle(currTime, codes, 15, 10);
    // executeMinuteCandle(currTime, codes, 240, 10);
  }, 60000);
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

// getCurrMinuteCandle();
// getPrevMinuteCandle();


const ONE_MINUTE = 60;
const ONE_HOUR = ONE_MINUTE * 60;

const isNeedToCalcOBV5 = (calc_time) => {
  return calc_time % (ONE_MINUTE * 5) === 0;
}

const isNeedToCalcOBV15 = (calc_time) => {
  return calc_time % (ONE_MINUTE * 15) === 0;
}

const isNeedToCalcOBV240 = (calc_time) => { 
  return (calc_time) % (ONE_MINUTE * 240) === 0;
}

/*
이슈. 데이터가 없을 경우의 시나리오 필요
*/
const calcObv5 = (type, code, candle) => {
  /* 
    1. candle.calc_timestamp를 기준으로 직전 계산 된 obv5 획득
    2. candle.calc_timestamp를 기준으로 직전의 캔들 5개 획득
    3. 계산
  */
  const criTime = candle.calc_timestamp;
  const prevObv5 = 0;
  const prevObv5_10 = 0;
  const prevCandles = [];

  const loopCnt = type === 'obv5' ? 5 : type === 'obv15' ? 15 : type === 'obv240' ? 240 : -1;
  if(loopCnt === -1){ throw new Error("Illegal argument Error - type must be ['obv5' || 'obv15' || 'obv240']"); }

  let prevVolume = 0;
  for(let i = 0; i < loopCnt; i++){
    if(prevCandles.length != loopCnt){} // 부족한 데이터 획득 시나리오 
    let vol = prevCandles[i].candle_acc_trade_volume;
    prevVolume += vol;
  }
}

const getPrevMinuteCandle2 = () => { 
  const criTimestamp = 1648566000000;
  const halfHour = 1000 * 60 * 30;
  let seq = 0;

  setInterval(()=>{
    const nextTime = new Date(criTimestamp + (halfHour * seq++));
    executeMinuteCandle(nextTime.toISOString(), codes, 1, 100);
    // executeMinuteCandle(currTime, codes, 15, 10);
    // executeMinuteCandle(currTime, codes, 240, 10);
  }, 60000);
}
getPrevMinuteCandle2();