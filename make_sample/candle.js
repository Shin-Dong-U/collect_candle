

export const makeCandlesDefaultData = async () => {
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