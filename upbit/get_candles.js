
import fetch from 'node-fetch';

export const getCandlesFromUpbit = async (code, minuteUnit, lastTime, count) => {
  const options = {method: 'GET', headers: {Accept: 'application/json'}};
  response = await fetch(`https://api.upbit.com/v1/candles/minutes/${minuteUnit}?market=${code}&to=${lastTime}&count=${count}`, options);
  return response;
}