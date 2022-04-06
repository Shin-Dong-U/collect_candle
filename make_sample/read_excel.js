import fs from 'fs';
import mysql from 'mysql';
import db_config from '../config/db_properties.js';

// 1회성 샘플데이터 OBV 기준점 밀어넣기
export const conn = mysql.createConnection({ host: db_config.host, user: db_config.user, password: db_config.password, database: db_config.database });
 
const doUpdate = async () => {
  const dir = 'C:\\Users\\yoyohub_1\\Documents\\chart\\upbit';
  const CRI_OBV5_CALC_TIME = 1648566000;
  const CRI_OBV15_CALC_TIME = 1648566000;
  const CRI_OBV240_CALC_TIME = 1648569600;

  const files = fs.readdirSync(dir);
  if(!files){ return; }
  
  const candles = {};

  for(let i = 0; i < files.length; i++){
    
    let file = files[i];
    // console.log(file);
    let fileNames = file.split(',');

    if(!fileNames[0].includes('UPBIT_') && !fileNames[0].includes('KRW')){ throw Error('ERROR - file name is not correct'); }

    let code = 'KRW-' + fileNames[0].replace('UPBIT_', '').replace('KRW', '');
    // console.log(code);
    
    let candle = candles[code];
    if(!candle){ candle = candles[code] = {}; }
    
    let filePath = dir + '\\' + file;
    let data = fs.readFileSync(filePath, 'utf8');
    let rows = data.split("\n");
    let result = [];
    for(let rowIdx in rows){
      let row = rows[rowIdx].split(",");
      if (rowIdx === "0") { 
        var columns = row; 
      }else {
        var _data = {};
        for (var columnIndex in columns) { 
          var column = columns[columnIndex];
          _data[column] = row[columnIndex];
        }
        result.push(_data);
      }
    }
    // console.log(result);

    let obv = 0;
    
    if(fileNames[1].includes('240.')){
      obv = Number(result.find(row => row.time == CRI_OBV240_CALC_TIME).OnBalanceVolume);
      candle['obv_240'] = {calc_timestamp : CRI_OBV240_CALC_TIME, obv_240 : obv};

    }else if(fileNames[1].includes('15.')){
      obv = Number(result.find(row => row.time == CRI_OBV15_CALC_TIME).OnBalanceVolume);
      candle['obv_15'] = {calc_timestamp : CRI_OBV15_CALC_TIME, obv_15 : obv};

    }else if(fileNames[1].includes('5.')){
      obv = Number(result.find(row => row.time == CRI_OBV5_CALC_TIME).OnBalanceVolume);
      candle['obv_5'] = {calc_timestamp : CRI_OBV5_CALC_TIME, obv_5 : obv};

    }else{
      throw Error('ERROR - file name is not correct', file);
    }

    if(!obv){
      throw Error('ERROR - obv is not correct', file);
    }
  }


  for(const[code, val] of Object.entries(candles)){
    // if(code != 'KRW-BTC'){ continue; }
    console.log(code + ' / ' + val.obv_5.obv_5 + ' / ' + val.obv_15.obv_15 + ' / ' + val.obv_240.obv_240)
    console.log(code + ' / ' + val.obv_5.calc_timestamp + ' / ' + val.obv_15.calc_timestamp + ' / ' + val.obv_240.calc_timestamp)
    
    conn.query(make_obv_update_sql(code, 5, val));
    conn.query(make_obv_update_sql(code, 15, val));
    conn.query(make_obv_update_sql(code, 240, val));
  }
}

const make_obv_update_sql = (code, type, candle) => {
  let column = '';
  let obv = 0;
  let calc_timestamp = 0;
  if(type == 5){
    column = 'obv_5';
    obv = candle.obv_5.obv_5;
    calc_timestamp = candle.obv_5.calc_timestamp;
  }else if(type == 15){
    column = 'obv_15';
    obv = candle.obv_15.obv_15;
    calc_timestamp = candle.obv_15.calc_timestamp;
  }else if(type ===240){
    column = 'obv_240';
    obv = candle.obv_240.obv_240;
    calc_timestamp = candle.obv_240.calc_timestamp;
  }
  
  const sql = `
    UPDATE candles
    SET ${column} = ${obv}
    WHERE market = '${code}' AND calc_timestamp = ${calc_timestamp}
  `;

  return sql;
}

doUpdate();