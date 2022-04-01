import mongoose from 'mongoose';

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
}, {
  timeseries: {
    timeField: 'candle_date_time_utc',
    granularity: 'minutes',
  }
});

// export const Candle = mongoose.model('minute_candle', candleSchema, 'minute_candles');
export const Candle15 = mongoose.model('minute_candle_15', candleSchema, 'minute_candles_15');
export const Candle240 = mongoose.model('minute_candle_240', candleSchema, 'minute_candles_240');

export const Candle = mongoose.model('minute_candle_5', candleSchema, 'minute_candles_5');