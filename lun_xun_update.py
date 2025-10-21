import akshare as ak
import redis
import time
import random
import pandas as pd
import numpy as np
from datetime import datetime
import sys
import requests

class StockDataUpdater:

    def __init__(self, redis_host='106.14.164.40', redis_port=6378, redis_db=3, password="0x2F746bC70f72aAF3340B8BbFd254fd91a3996218"):
        self.redis = redis.Redis(
            host=redis_host, port=redis_port, db=redis_db,
            password=password, decode_responses=True
        )
        self._test_connection()
    
    def _test_connection(self):
        try:
            self.redis.ping()
            print("✅ Redis连接成功")
        except Exception as e:
            print(f"❌ Redis连接失败: {e}")
            raise
    
    def get_all_stock_codes(self):
        """从Redis获取所有股票代码"""
        try:
            codes = self.redis.smembers("stocks:all_codes")
            print(f"📊 从Redis获取到 {len(codes)} 只股票代码")
            return list(codes)
        except Exception as e:
            print(f"❌ 获取股票代码失败: {e}")
            return []
    
    def safe_float_convert(self, value, default=0.0):
        """安全转换为float - 修复版"""
        try:
            # 处理None和空值
            if value is None or value == '':
                return default
            
            # 处理字符串类型的NaN
            if isinstance(value, str) and value.lower() in ['nan', 'null', 'none', '']:
                return default
            
            # 转换为float
            value_float = float(value)
            
            # 使用numpy检查NaN和inf
            if np.isnan(value_float) or np.isinf(value_float):
                return default
                
            return value_float
            
        except (ValueError, TypeError) as e:
            print(f"数值转换失败: {value} -> {e}")
            return default
    
    def get_stock_real_time_data(self, stock_code):
        """获取单只股票的实时数据"""
        try:
            # 添加随机延迟，避免请求过于频繁
            time.sleep(random.uniform(0.1, 0.3))
            
            # 判断市场前缀
            if stock_code.startswith('6'):
                full_code = f"sh{stock_code}"
            else:
                full_code = f"sz{stock_code}"
            
            # 使用腾讯财经接口
            url = f"http://qt.gtimg.cn/q={full_code}"
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36'
            }
            
            response = requests.get(url, headers=headers, timeout=10)
            
            if response.status_code == 200 and response.text:
                data = response.text.split('~')
                if len(data) > 40:
                    current_price = self.safe_float_convert(data[3])  # 当前价格
                    pe_ratio = self.safe_float_convert(data[39])     # 市盈率
                    
                    return {
                        'price': current_price,
                        'pe': pe_ratio,
                        'success': True
                    }
            
            return {'success': False, 'error': '接口返回数据异常'}
            
        except Exception as e:
            return {'success': False, 'error': str(e)}
    
    def update_single_stock(self, stock_code):
        """更新单只股票数据"""
        try:
            # 获取实时数据
            real_time_data = self.get_stock_real_time_data(stock_code)
            
            if not real_time_data['success']:
                print(f"❌ {stock_code} 获取实时数据失败: {real_time_data.get('error')}")
                return False
            
            update_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            # 获取股票名称（从Redis中）
            stock_info = self.redis.hgetall(f"stock:hash:{stock_code}")
            stock_name = stock_info.get('name', '未知')
            
            # 更新Hash数据
            update_data = {
                'price': str(real_time_data['price']),
                'pe': str(real_time_data['pe']),
                'update_time': update_time,
                'last_sync': update_time
            }
            self.redis.hset(f"stock:hash:{stock_code}", mapping=update_data)
            
            # 更新价格排序集合
            if real_time_data['price'] > 0:
                self.redis.zadd("stocks:by_price", {stock_code: real_time_data['price']})
            
            # 更新PE排序集合
            if real_time_data['pe'] > 0 and real_time_data['pe'] < 10000:
                self.redis.zadd("stocks:by_pe", {stock_code: real_time_data['pe']})
            
            print(f"✅ {stock_code} {stock_name} 更新成功: 价格{real_time_data['price']}, PE{real_time_data['pe']}")
            return True
            
        except Exception as e:
            print(f"❌ {stock_code} 更新失败: {e}")
            return False
    
    def batch_update_stocks(self, batch_size=50, delay_between_batches=3):
        """批量更新股票数据"""
        all_codes = self.get_all_stock_codes()
        if not all_codes:
            print("❌ 没有找到股票代码，无法更新")
            return
        
        total_count = len(all_codes)
        success_count = 0
        fail_count = 0
        
        print(f"🚀 开始批量更新 {total_count} 只股票数据...")
        print(f"📦 批次大小: {batch_size}, 批次间隔: {delay_between_batches}秒")
        
        # 分批处理
        for i in range(0, total_count, batch_size):
            batch_codes = all_codes[i:i + batch_size]
            batch_num = i // batch_size + 1
            total_batches = (total_count + batch_size - 1) // batch_size
            
            print(f"\n🔄 正在处理第 {batch_num}/{total_batches} 批次 ({len(batch_codes)}只股票)...")
            
            batch_success = 0
            batch_fail = 0
            
            for stock_code in batch_codes:
                if self.update_single_stock(stock_code):
                    batch_success += 1
                    success_count += 1
                else:
                    batch_fail += 1
                    fail_count += 1
            
            print(f"📊 第{batch_num}批次完成: 成功{batch_success}, 失败{batch_fail}")
            
            # 如果不是最后一批，等待一段时间
            if i + batch_size < total_count:
                print(f"⏳ 等待{delay_between_batches}秒后继续下一批次...")
                time.sleep(delay_between_batches)
        
        # 更新元数据
        update_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        self.redis.set("stocks:last_update", update_time)
        self.redis.set("stocks:last_batch_update", update_time)
        
        print(f"\n🎉 批量更新完成！")
        print(f"📈 统计结果:")
        print(f"   总处理: {total_count} 只")
        print(f"   成功: {success_count} 只")
        print(f"   失败: {fail_count} 只")
        print(f"   成功率: {success_count/total_count*100:.2f}%")
        print(f"   更新时间: {update_time}")
    
    def continuous_update(self, interval_minutes=5):
        """持续轮训更新"""
        print(f"🔄 启动持续更新模式，每{interval_minutes}分钟更新一次")
        
        try:
            update_count = 0
            while True:
                update_count += 1
                print(f"\n{'='*50}")
                print(f"🕒 第{update_count}轮更新开始: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
                
                self.batch_update_stocks(batch_size=50, delay_between_batches=3)
                
                print(f"⏰ 等待 {interval_minutes} 分钟后进行下一轮更新...")
                time.sleep(interval_minutes * 60)
                
        except KeyboardInterrupt:
            print("\n⏹️ 用户停止持续更新")
    
    def show_current_stats(self):
        """显示当前数据统计"""
        print(f"\n📊 当前Redis数据统计:")
        total_stocks = self.redis.scard("stocks:all_codes")
        last_update = self.redis.get("stocks:last_update") or "从未更新"
        
        print(f"股票总数: {total_stocks}")
        print(f"最后更新: {last_update}")
        print(f"价格排序集合: {self.redis.zcard('stocks:by_price')}")
        print(f"PE排序集合: {self.redis.zcard('stocks:by_pe')}")
        
        # 显示几只股票的最新数据
        print(f"\n🔍 最新数据示例:")
        sample_codes = list(self.redis.smembers("stocks:all_codes"))[:3]
        for code in sample_codes:
            data = self.redis.hgetall(f"stock:hash:{code}")
            name = data.get('name', '未知')
            price = data.get('price', '未知')
            pe = data.get('pe', '未知')
            update_time = data.get('update_time', '未知')
            print(f"  {code} {name}: 价格{price}, PE{pe}, 更新{update_time}")

# 简化使用
def quick_update():
    """快速启动更新"""
    updater = StockDataUpdater()
    updater.show_current_stats()
    
    # 直接开始批量更新
    updater.batch_update_stocks(batch_size=50, delay_between_batches=3)

if __name__ == "__main__":
    quick_update()