# -*- coding: utf-8 -*-
from Ashare import *
import pandas as pd
import json
import sys
import os
from stock_redis_manager import query_all_stock_codes,batch_update_stock_kdata ,batch_update_redis_hash,query_redis_hash

# 添加当前目录到路径
sys.path.append(os.path.abspath('.'))
# 数据缓存最大值
limit_num=10

def get_stock_k_data(stock_code='000002',frequency='1d',count=1):
    """
    通过Ashare获取单只股票最新价格
    :param stock_code: 股票代码，如'000001'
    :return: float or None
    """
    try:
        # 转换股票代码格式 (sh000001, sz000001等)
        if stock_code.startswith(('5', '6', '9')):
            # 上海股票
            ashare_code = f"sh{stock_code}"
        else:
            # 深圳股票
            ashare_code = f"sz{stock_code}"

        # 使用Ashare的get_price获取实时价格
        df = get_price(ashare_code, frequency=frequency, count=count)  # 获取最近1天的日线数据
       
        

        if df is not None and not df.empty:
            return df
        else:
            print("获取数据失败")
            return None

    except Exception as e:
        print(f"获取股票价格失败: {e}")
        return None


    

def update_all_stock_k_data(limit=-1,count=10,frequency='1d'):
    stock_codes = query_all_stock_codes(limit=limit)  # 获取前5只股票

    for stock_code in stock_codes:

    # stock_code='000002'
        df=get_stock_k_data(stock_code=stock_code,frequency=frequency,count=count)
        if df is None:
            continue
        print(df)
        updates=[]
        for index, row in df.iterrows():

            update={}
            update['stock_code']=stock_code
            # print(f"索引: {index}, open: {row['open']}, 年龄: {row['年龄']}")
            update['field']=str(index)
            # print(f'{str(index)}',f"{row.to_json()}")

            update['value']=row.to_json()
            updates.append(update)
        # print(updates)
        batch_update_stock_kdata(updates, frequency='1d')

# def update_all_stock_price(limit=-1):
#     stock_codes = query_all_stock_codes(limit=limit)  # 获取前5只股票


#     if stock_codes:
#         print(f"获取到股票代码: {stock_codes}")

#         # 获取多个股票的实时价格
#         prices = {}
#         updates=[]
#         for code in stock_codes:
#             price = get_stock_price(code)
            
#             if price:
#                 prices[code] = price
#                 update={}
#                 update["stock_code"]=code
#                 update['field']='price'
#                 data = query_redis_hash(code)

#                 af_price=list(json.loads(data[update['field']]))
#                 af_price.insert(0,price)
#                 if len(af_price)>limit_num:
#                     af_price.pop()

#                 print(af_price)
#                 update['value']=json.dumps(af_price)
#                 updates.append(update)
#                 print(f"股票 {code} 最新价格: {price}")
#             else:
#                 print(f"获取股票 {code} 价格失败")

#         print(f"\n成功获取 {len(prices)} 只股票的价格")
#         print(updates)
#         batch_update_redis_hash(updates=updates)

#     else:
#         print("未获取到股票代码")
# 使用示例
if __name__ == "__main__":
    # update_all_stock_price(-1)
    # update_stock_price(stock_codes=['601939'])
    update_all_stock_k_data(100)
    


    