// 어제 00시 타임스탬프 구하기
export const getYesterdayTimestamp = () => {
  const ONE_DAY = 1000 * 60 * 60 * 24;
  
  const date = new Date();
  date.setDate(date.getDate() - 1);
  let timestamp = date.getTime();

  return timestamp - (timestamp % ONE_DAY);
}