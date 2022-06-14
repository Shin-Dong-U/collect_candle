import {getPrevTimestamp} from './common/utils.js';
// import {getPrevMinuteCandle, getCurrMinuteCandle, retryFailCandle} from './jobs/get_obv.js';
import {getPrevMinuteCandle, getCurrMinuteCandle, retryFailCandle} from './jobs/get_obv.js';

// const startTime = 1648566000000; //  2022-03-30 00:00
let timestampParam = process.argv[2];
const startTime = await getPrevTimestamp(timestampParam);

console.log(startTime);

/*
1. 입력 된 시간부터 현재까지 Candle 획득
2. 1이 완료 되었다면 현재 시간부터 약 1분 단위로 요청
3. 주기적으로 실패한 데이터가 있는지 확인 후 재 요청
*/
const start = (startTime) => {
  getPrevMinuteCandle(startTime, getCurrMinuteCandle, 3);
  retryFailCandle();
}

start(startTime);