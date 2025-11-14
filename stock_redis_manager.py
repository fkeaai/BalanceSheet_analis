# -*- coding: utf-8 -*-
import redis
import json
import sys
import os

# 添加当前目录到路径
sys.path.append(os.path.abspath('.'))
from redis_utils import get_redis
data_base_num=4
r = get_redis(data_base=data_base_num)

def stock_should_update(stock_code,frequency='1d', r=None):
    """
    查询
    :param stock_code: 股票代码（6位数字）
    :param field: 具体的字段，如果为None则返回所有字段
    :param r: Redis连接对象，如果为None则创建新的连接
    :return: 查询结果
    """
    if r is None:
        r = get_redis(data_base=data_base_num)

    # 构造hash key
    hash_key = f"stocks:k:{stock_code.zfill(6)}:{frequency}"

    try:
        if frequency is not None:
            # 获取特定字段
            keys = r.hkeys(hash_key)
            from datetime import datetime,date

# 获取当前日期时间并格式化
            current_datetime = datetime.combine(date.today(), datetime.min.time())
            formatted_time = current_datetime.strftime('%Y-%m-%d %H:%M:%S')
            # print(date)
            res=str(formatted_time) not in keys
            print(f'{stock_code} should update {res}')
            
            return formatted_time not in keys
    except Exception as e:
        print(f"查询hash失败: {e}")
        return None
    
def query_redis_hash(stock_code, field=None, r=None):
    """
    查询Redis hash数据
    :param stock_code: 股票代码（6位数字）
    :param field: 具体的字段，如果为None则返回所有字段
    :param r: Redis连接对象，如果为None则创建新的连接
    :return: 查询结果
    """
    if r is None:
        r = get_redis(data_base=data_base_num)

    # 构造hash key
    hash_key = f"stock:{stock_code.zfill(6)}"

    try:
        if field is None:
            # 获取所有字段
            result = r.hgetall(hash_key)
            return result
        else:
            # 获取特定字段
            result = r.hget(hash_key, field)
            return result
    except Exception as e:
        print(f"查询hash失败: {e}")
        return None

def query_all_stock_codes(r=None,limit=-1):
    """
    查询并打印所有股票数据
    :param r: Redis连接对象
    """
    all_codes = []

    if r is None:
        r = get_redis(data_base=data_base_num)

    try:
        # 使用scan获取所有key，避免keys的性能问题
        cursor = 0
        while True:
            cursor, keys = r.scan(cursor, match='stock:*', count=1000)
            for key in keys:
                # print(key)
                # if key.isdigit() :
                all_codes.append(key.replace("stock:",""))
                if len(all_codes)==limit:
                    break
            if len(all_codes)==limit:
                    break      
            if cursor == 0:
                break
    except Exception as e:
        print(f"查询所有数据失败: {e}")

    return all_codes
def query_and_print_all_data(r=None):
    """
    查询并打印所有股票数据
    :param r: Redis连接对象
    """
    if r is None:
        r = get_redis(data_base=data_base_num)

    try:
        # 使用scan获取所有key，避免keys的性能问题
        all_codes = []
        cursor = 0
        while True:
            cursor, keys = r.scan(cursor, match='stock:*', count=1000)
            for key in keys:
                print(key)
                # if key.isdigit() :
                all_codes.append(key)
            if cursor == 0:
                break

        print(f"总共有 {len(all_codes)} 只股票")

        i=0
        for code in all_codes:
            i+=1
            code=code.replace("stock:","")

            data = query_redis_hash(code, r=r)
            print(f"\n股票代码: {code}")

            if data:
                print(f"第{i}支股票")
                for field, value in data.items():
                    print(f"  {field}: {value}")
            else:
                print("  无数据")

        # 打印统计信息
        print(f"\n统计信息:")
        print(f"总股票数: {len(all_codes)}")
        print(f"最后更新: {r.get('stocks:last_update') or '未知'}")

    except Exception as e:
        print(f"查询所有数据失败: {e}")

def query_and_del_0_price_data(r=None):
    """
    查询并打印所有股票数据
    :param r: Redis连接对象
    """
    if r is None:
        r = get_redis(data_base=data_base_num)

    try:
        # 使用scan获取所有key，避免keys的性能问题
        all_codes = []
        cursor = 0
        while True:
            cursor, keys = r.scan(cursor, match='stock:*', count=1000)
            for key in keys:
                # print(key)
                # if key.isdigit() :
                all_codes.append(key)
            if cursor == 0:
                break

        print(f"总共有 {len(all_codes)} 只股票")

        i=0
        for code in all_codes:
            i+=1
            code=code.replace("stock:","")

            data = query_redis_hash(code, r=r)
            # print(f"\n股票代码: {code}")

            if data:
                print(f"第{i}支股票")
                for field, value in data.items():
                    
                    if field=="price" and int(json.loads(value)[0])==0:

                        print(f"当前可删除股票代码{code}")
                        delete_redis_hash(code,r)
            else:
                print("  无数据")

        # 打印统计信息
        print(f"\n统计信息:")
        print(f"总股票数: {len(all_codes)}")
        print(f"最后更新: {r.get('stocks:last_update') or '未知'}")

    except Exception as e:
        print(f"查询所有数据失败: {e}")

def update_redis_hash_field(stock_code, field, value, r=None):
    """
    修改Redis hash中的单个字段
    :param stock_code: 股票代码（6位数字）
    :param field: 要修改的字段名
    :param value: 新值
    :param r: Redis连接对象
    :return: bool
    """
    if r is None:
        r = get_redis(data_base=data_base_num)

    # 构造hash key
    hash_key = f"stock:{stock_code.zfill(6)}"

    try:
        result = r.hset(hash_key, field, value)
        if result == 1:
            print(f"字段 {field} 更新成功")
        else:
            print(f"字段 {field} 更新（新字段）")
        return True
    except Exception as e:
        print(f"修改字段失败: {e}")
        return False

def batch_update_redis_hash(updates, r=None):
    """
    批量更新Redis hash字段，使用pipeline提高效率
    :param updates: list of dicts, 每个dict包含'stock_code', 'field', 'value'
    :param r: Redis连接对象
    :return: bool
    """
    if r is None:
        r = get_redis(data_base=data_base_num)

    try:
        pipe = r.pipeline()
        for update in updates:
            stock_code = update['stock_code']
            field = update['field']
            value = update['value']

            # 构造hash key
            hash_key = f"stock:{stock_code.zfill(6)}"
            pipe.hset(hash_key, field, value)

        # 执行批量操作
        results = pipe.execute()
        print(f"批量更新完成，共更新 {len(results)} 个字段")
        return True

    except Exception as e:
        print(f"批量更新失败: {e}")
        return False
def batch_update_stock_kdata(updates ,frequency='1d',r=None):
    """
    批量更新Redis hash字段，使用pipeline提高效率
    :param updates: list of dicts, 每个dict包含'stock_code', 'field', 'value'
    :param r: Redis连接对象
    :return: bool
    """
    if r is None:
        r = get_redis(data_base=data_base_num)

    try:
        pipe = r.pipeline()
        for update in updates:
            stock_code = update['stock_code']
            field = update['field']
            value = update['value']

            # 构造hash key
            hash_key = f"stocks:k:{stock_code.zfill(6)}:{frequency}"
            pipe.hset(hash_key, field, value)

        # 执行批量操作
        results = pipe.execute()
        print(f"批量更新完成，共更新 {len(results)} 个字段")
        return True

    except Exception as e:
        print(f"批量更新失败: {e}")
        return False

def delete_redis_hash(stock_code, r=None):
    """
    删除Redis hash数据（整条股票记录）
    :param stock_code: 股票代码（6位数字）
    :param r: Redis连接对象
    :return: bool
    """
    if r is None:
        r = get_redis()

    # 构造hash key
    hash_key = f"stock:{stock_code.zfill(6)}"

    try:
        result = r.delete(hash_key)
        if result == 1:
            print(f"股票 {stock_code} 数据删除成功")
            return True
        else:
            print(f"股票 {stock_code} 数据不存在")
            return False
    except Exception as e:
        print(f"删除数据失败: {e}")
        return False

# 使用示例
if __name__ == "__main__":
    stock_code="000002"
    # 查询整个hash
    data = query_redis_hash("000002")
    print("Hash 000002 的所有数据:", data)

    # 查询特定字段
    # name = query_redis_hash("000001", "name")
    # print("Hash 000001 的name字段:", name)

    prices=json.loads(data['price'])
    prices[0]=prices[0]+1
    prices=json.dumps(prices)
    print(prices)
    # 修改字段
    update_redis_hash_field("000002", "price", prices,r=r)
    # batch_update_stock_kdata()
    # 查询并打印所有数据
    # print("\n" + "="*50)
    # query_and_print_all_data(r)
    # 删除price 为0 的股票
    # query_and_del_0_price_data(r)
    # delete_redis_hash("000003", r)
    update={}
    updates=[]

    update['stock_code']="000001"
    update['field']='price'
    update['value']=json.dumps([13.44])
    updates.append(update)
           
    # batch_update_redis_hash(updates, r=None)
    stock_should_update(stock_code,frequency='1d')


    


