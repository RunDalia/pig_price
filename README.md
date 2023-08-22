## 돈육 가격 예측 프로젝트

국내 시장에서 거래되고 있는 돼지고기의 가격이 코로나 이후, 그 이전과는 다른 흐름을 보이게 되어 이를 분석하여 보다 정확히 돈육 가격을 예측하여 가장 저렴한 날짜에 거래를 하고자 하는 고객사로부터 해당 프로젝트를 의뢰받아 참여하게 된 프로젝트입니다. 

1. tb_day_oksusu.py

  
  돼지의 사료가 되는 옥수수의 가격(미국)을 매일 자동으로 크롤링하는 코드 개발
  (https://kr.investing.com/commodities/us-corn-historical-data)

2. tb_day_dollar.py

  
  옥수수 가격이 달러 단위로 수집되기 때문에 이를 계산하기 위해 매일 변화하는 환율을 자동으로 수집하는 크롤링 코드 개발
  (https://kr.investing.com/currencies/usd-krw-historical-data)

3. tb_day_trade.py

  
  돼지 수급동향(출하 관련) 일자, 판정 두수(마리), 거래정육량(톤)을 매일 자동으로 수집하는 크롤링 코드 개발
  (https://www.ekapepia.com/priceStat/forwardTrendStatus.do?menuId=menu100249&boardInfoNo=)

4. tb_day_bypart_local_price.py

  
  돼지 산지가격,도매평균가격을 매일 자동으로 수집하는 크롤링 코드 개발
  (https://www.ekapepia.com/priceStat/distrPricePork.do?menuId=menu100034&boardInfoNo=)

5. domestic_price_analysis_NEW.ipynb

  
  수집된 데이터를 기반으로 데이터 분석한 알고리즘입니다. 
