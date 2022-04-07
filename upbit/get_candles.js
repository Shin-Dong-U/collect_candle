
import fetch from 'node-fetch';

const getCandlesFromUpbit = async (code, minuteUnit, lastTime, count) => {
  const options = {method: 'GET', headers: {Accept: 'application/json'}};
  let response = await fetch(`https://api.upbit.com/v1/candles/minutes/${minuteUnit}?market=${code}&to=${lastTime}&count=${count}`, options);
  return response;
}

// let res = await getCandlesFromUpbit('KRW-BTC', 1, '2022-04-07T00:00:00Z', 10);
// console.log(await res.json());

export default getCandlesFromUpbit;