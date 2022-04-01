import fetch from 'node-fetch';
const codes = [];

/*
const getKrwMarketCodes = async () => {
  if(codes.length != 0){ return codes;}
  const options = {method: 'GET', headers: {Accept: 'application/json'}};
  const response = await (await fetch('https://api.upbit.com/v1/market/all?isDetails=false', options)).json();

  Array.from(response).filter(e => e.market.startsWith("KRW-")).forEach((e)=> {
    codes.push(e.market);
  });

  return codes;
}
*/

const getKrwMarketCodes = async () => {
  if(codes.length != 0){ return codes;}
  codes.push('KRW-BTC');
  codes.push('KRW-SAND');
  codes.push('KRW-SRM');
  codes.push('KRW-KNC');
  codes.push('KRW-ICX');
  
  return codes;
}

export default getKrwMarketCodes;