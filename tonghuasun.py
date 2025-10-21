import akshare as ak
import redis
import pandas as pd
import time
import random
from datetime import datetime

class StableStockUpdater:

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
    
    def robust_get_stock_data(self, max_retries=3):
        """稳健的获取股票数据方法，包含重试机制"""
        for attempt in range(max_retries):
            try:
                print(f"📈 尝试获取股票数据 (第{attempt + 1}次)...")
                
                # 添加随机延迟，避免请求过于规律
                time.sleep(random.uniform(1, 3))
                
                # 获取数据
                stock_df = ak.stock_zh_a_spot_em()
                
                if stock_df is not None and len(stock_df) > 0:
                    print(f"✅ 成功获取 {len(stock_df)} 只股票数据")
                    return stock_df
                else:
                    print("⚠️ 获取到的数据为空，准备重试...")
                    
            except Exception as e:
                print(f"❌ 第{attempt + 1}次获取失败: {e}")
                if attempt < max_retries - 1:
                    wait_time = (attempt + 1) * 5  # 递增等待时间
                    print(f"⏳ 等待{wait_time}秒后重试...")
                    time.sleep(wait_time)
                else:
                    print("🚨 所有重试均失败")
        
        return None
    
    def get_stock_data_with_fallback(self):
        """主备方案获取数据"""
        print("🔄 使用主方案获取数据...")
        stock_df = self.robust_get_stock_data()
        
        if stock_df is None:
            print("🔄 主方案失败，尝试备用方案...")
            stock_df = self.fallback_get_stock_data()
        
        return stock_df
    
    def fallback_get_stock_data(self):
        """备用数据获取方案"""
        try:
            # 方案1: 尝试使用其他接口
            print("尝试使用备用接口...")
            stock_df = ak.stock_info_a_code_name()
            
            if stock_df is not None:
                # 如果有基础股票列表，可以批量获取价格
                print("获取到基础股票列表，尝试批量获取价格...")
                # 这里可以添加批量获取价格的逻辑
                pass
                
            return stock_df
            
        except Exception as e:
            print(f"备用方案也失败: {e}")
            return None
    
    def update_stocks_to_redis(self):
        """更新股票数据到Redis"""
        stock_df = self.get_stock_data_with_fallback()
        
        if stock_df is None:
            print("🚨 无法获取股票数据，请检查网络或稍后重试")
            return False
        
        try:
            # 选择需要的列
            required_columns = ['代码', '名称', '最新价', '市盈率-动态']
            
            # 检查列是否存在
            available_columns = [col for col in required_columns if col in stock_df.columns]
            if not available_columns:
                print("❌ 数据列不匹配，可用列:", stock_df.columns.tolist())
                return False
            
            stock_df = stock_df[available_columns]
            update_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            print(f"💾 开始更新 {len(stock_df)} 只股票数据到Redis...")
            
            # 使用pipeline批量操作
            pipe = self.redis.pipeline()
            success_count = 0
            
            for index, row in stock_df.iterrows():
                try:
                    stock_code = str(row['代码']).zfill(6)  # 确保6位代码
                    
                    stock_data = {
                        'code': stock_code,
                        'name': row['名称'],
                        'price': str(round(float(row['最新价']), 2)),
                        'update_time': update_time
                    }
                    
                    # 添加PE数据（如果存在）
                    if '市盈率-动态' in row and pd.notna(row['市盈率-动态']):
                        stock_data['pe'] = str(round(float(row['市盈率-动态']), 2))
                    
                    # 使用Hash存储
                    pipe.hset(f"stock:{stock_code}", mapping=stock_data)
                    
                    # 更新价格排序集合
                    price = float(stock_data['price'])
                    pipe.zadd("stocks:sort:price", {stock_code: price})
                    
                    success_count += 1
                    
                    # 每100条执行一次
                    if success_count % 100 == 0:
                        pipe.execute()
                        print(f"🔄 已处理 {success_count} 条数据...")
                        pipe = self.redis.pipeline()
                        
                except Exception as e:
                    print(f"⚠️ 处理股票 {row.get('代码', 'unknown')} 时出错: {e}")
                    continue
            
            # 执行剩余命令
            pipe.execute()
            
            # 更新元数据
            self.redis.set("stocks:metadata:last_update", update_time)
            self.redis.set("stocks:metadata:total_count", success_count)
            
            print(f"✅ 更新完成！成功处理 {success_count} 只股票")
            print(f"🕒 更新时间: {update_time}")
            
            return True
            
        except Exception as e:
            print(f"❌ 更新Redis失败: {e}")
            return False

    def get_redis_stats(self):
        """获取Redis统计信息"""
        try:
            total = self.redis.get("stocks:metadata:total_count") or 0
            last_update = self.redis.get("stocks:metadata:last_update") or "未知"
            
            print(f"\n📊 Redis数据统计:")
            print(f"股票总数: {total}")
            print(f"最后更新: {last_update}")
            
            # 获取几个示例数据
            sample_keys = self.redis.keys("stock:0*")[:3]  # 前3个
            for key in sample_keys:
                data = self.redis.hgetall(key)
                print(f"示例: {key} -> {data}")
                
        except Exception as e:
            print(f"获取统计信息失败: {e}")

# 使用示例
if __name__ == "__main__":
    # 初始化
    updater = StableStockUpdater()
    
    # 更新数据
    success = updater.update_stocks_to_redis()
    
    if success:
        # 显示统计信息
        updater.get_redis_stats()
    else:
        print("❌ 数据更新失败，请检查网络连接后重试")