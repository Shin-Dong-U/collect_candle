import mongoose from 'mongoose';
import fs from 'fs';

/*
1회성 샘플데이터 OBV 기준점 밀어넣기
 */

const dir = 'C:\\Users\\yoyohub_1\\Documents\\chart\\upbit';
const CRI_OBV5_CALC_TIME = 1648566000;
const CRI_OBV15_CALC_TIME = 1648566000;
const CRI_OBV240_CALC_TIME = 1648569600;

const files = fs.readdirSync(dir);

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


const connUrl = 'mongodb://192.168.219.106:27017/crypto';
mongoose.connect(connUrl);

var db = mongoose.connection;

await db.once("open", function() {
  console.log("MongoDB database connection established successfully");
});


// console.log(candles);
// console.log(candles['KRW-1INCH']['obv_240']);
// console.log(Object.keys(candles).length);
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

let sample = {"market":"TEST","candle_date_time_utc":"2019-12-31T23:59:59.000Z","candle_date_time_kst":"2019-12-31T15:59:59.000Z","opening_price":0,"high_price":0,"low_price":0,"trade_price":0,"calc_timestamp":1648566000,"timestamp":1648566000,"candle_acc_trade_price":0,"candle_acc_trade_volume":0,"unit":0,"obv_5":0,"obv_5_p10":0,"obv_15":0,"obv_15_p10":0,"obv_240":0,"obv_240_p10":0}
let candle1 = new Candle({market: 'TEST', data: [sample]});
// candle1.save();
// db.collection("minute_candles").findOneAndUpdate({"market": "TEST"}, {$push: {data: sample}}, {upsert:true});
// Candle.updateOne( 
//   {
//     market: "KRW-BTC", 
//     data: {$elemMatch:{calc_timestamp: {$eq: CRI_OBV5_CALC_TIME} }} 
//   },
//   {
//     $set: {'data.$.obv_5': 123456789}
//   },
// )

  // var a = await Candle.updateOne(
  //   {
  //         market: "TEST", 
  //         data: {$elemMatch:{calc_timestamp: {$eq: CRI_OBV5_CALC_TIME} }} 
  //       },
  //       {
  //         $set: {'data.$.obv_5': 123456789}
  //       },
  // )
  // console.log(a);

for(const[code, val] of Object.entries(candles)){
  // console.log(code, ' ### ', val);
  // if(code != 'KRW-BTC'){ continue; }
  var o5 = await Candle.updateOne(
    {
      market: code, 
      data: {$elemMatch:{calc_timestamp: {$eq: val.obv_5.calc_timestamp} }} 
    },
    {
      $set: {'data.$.obv_5': val.obv_5.obv_5}
    },
  );
  
  var o15 = await Candle.updateOne(
    {
      market: code, 
      data: {$elemMatch:{calc_timestamp: {$eq: val.obv_15.calc_timestamp} }} 
    },
    {
      $set: {'data.$.obv_15': val.obv_15.obv_15}
    },
  );

  var o240 = await Candle.updateOne(
    {
      market: code, 
      data: {$elemMatch:{calc_timestamp: {$eq: val.obv_240.calc_timestamp} }} 
    },
    {
      $set: {'data.$.obv_240': val.obv_240.obv_240}
    },
  );
  if(o5.modifiedCount != 1){console.log(code, " / obv_5 업데이트 오류 / ", o5);}
  if(o15.modifiedCount != 1){console.log(code, " / obv_15 업데이트 오류 / ", o15);}
  if(o240.modifiedCount != 1){console.log(code, " / obv_240 업데이트 오류 / ", o240);}
}
