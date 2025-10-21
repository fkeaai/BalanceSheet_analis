
import redis
import json
import sys
import os
from enum import Enum
sys.path.append(os.path.abspath('.'))
# from useful_utils.excel_utils import csv_to_json
redis_configs={
    '106.14.164.40':["0x2F746bC70f72aAF3340B8BbFd254fd91a3996218",6378,1,True],
    '61.147.115.210':["wxb420651f4f7afea5",6378,1,True]
}
# list_keys
V2_LENDING_POOLS="V2_LENDING_POOLS"
V3_LENDING_POOLS="V3_LENDING_POOLS"
# list
X_USER_POSTS="X_USER_POSTS"
# set
X_USERNAMES="X_USERNAMES"
# set
X_POST_DETAILS="X_POST_DETAILS"

# 

def get_redis(ip="106.14.164.40",data_base=1):
    # r=redis.StrictRedis(host='127.0.0.1',password="0x894eb2F36769be80171F9D700B39695C1500B9f6", port=6379, db=1, decode_responses=True)
    r=redis.StrictRedis(host=ip,password=redis_configs[ip][0], port=redis_configs[ip][1], db=data_base, decode_responses=redis_configs[ip][3])
    return r


def hset_json_to_hash(redis_name="",key="",one_json={},cur_redis=get_redis("106.14.164.40")):
    # cur_redis.rpush(redis_name,json.dumps(one_json))
    cur_redis.hset(redis_name,key,json.dumps(one_json))
    
    # print(cur_redis.lrange(redis_name, cur_redis.llen(redis_name)-1, cur_redis.llen(redis_name)))
def lrange_one_json(redis_name="",one_json={},cur_redis=get_redis("106.14.164.40")):
    cur_redis.rpush(redis_name,json.dumps(one_json))
    print(cur_redis.lrange(redis_name, cur_redis.llen(redis_name)-1, cur_redis.llen(redis_name)))

def lrange_batch_json(redis_name="",jsons=[],cur_redis=get_redis("106.14.164.40")):
    cur_redis=get_redis()

    s_jsons=[]
    while len(jsons)>0:
        one =jsons.pop(0)
        s_jsons.append(json.dumps(one))


    cur_redis.rpush(redis_name,*s_jsons)

    print(cur_redis.llen(redis_name))

def remove_from_list(redis_name="",value="",cur_redis=get_redis()):
    # res=cur_redis.get(redis_name)
    cur_redis.lrem(redis_name,0,str(value))
    # print(cur_redis.llen(redis_name))
    # for one in res:
    #     one=str(json.loads(one))
    # return res
def get_page_json(redis_name="",limit=100,page=0,cur_redis=get_redis()):
    # res=cur_redis.get(redis_name)
    res=cur_redis.lrange(redis_name, page*limit, (page+1)*limit-1)

    # print(cur_redis.llen(redis_name))
    for one in res:
        one=str(json.loads(one))
    return res
# telnet 106.14.164.40 6379
# def insert_token_holder(token_name="",redis_ip="61.147.115.210"):
#     # token_name="usdt"
#     # token_name="dai"
#     # token_name="pepe"
#     # token_name="usdc"
#     # token_name="weth"
#     redis_name=token_name+"_holder"
#     c_r=get_redis(redis_ip)
#     c_r.delete(redis_name)

#     res=csv_to_json("useful_utils/"+token_name+"_holder_2025205.csv")
#     print(res)
#     res=json.loads(res)
#     # for one in res:
        
#     # print(type(res[0]))

#     lrange_batch_json(redis_name=redis_name,jsons=res,cur_redis=c_r)

def is_in_set(redis_name="",value="",cur_redis=get_redis()):
    is_member = cur_redis.sismember(redis_name, value)
    print("Is 'element1' a member of the set?", is_member)

    return is_member

def lpush_to_list(redis_name="",value="",cur_redis=get_redis()):

    cur_redis.lpush(redis_name,value)
    
    # all_elements = cur_redis.hkeys(redis_name)
    # for k, v in k_vs.items():
    #     cur_redis.hset(redis_name,k,v)
# 适合 name --> {"key","value"}
def add_to_hash(redis_name="",k_vs={},cur_redis=get_redis()):

    all_elements = cur_redis.hkeys(redis_name)
    for k, v in k_vs.items():
        cur_redis.hset(redis_name,k,v)
    # r.hset('user:1', 'age', 30)
    # return all_elements

def get_keys_from_hash(redis_name="",cur_redis=get_redis()):
    all_elements = cur_redis.hkeys(redis_name)
    return all_elements

def get_from_hash(redis_name="",key="",cur_redis=get_redis()):
    all_elements =  cur_redis.hgetall(redis_name)
    return all_elements
def get_all_from_hash(redis_name="",cur_redis=get_redis()):
    all_elements =  cur_redis.hgetall(redis_name)
    return all_elements
    
def get_all_from_set(redis_name="",cur_redis=get_redis()):
    all_elements = cur_redis.smembers(redis_name)
    return all_elements


def remove_from_set(redis_name="",values=[],cur_redis=get_redis()):
    while len(values)>0:

        cur_redis.srem(redis_name,values.pop(0))

def add_to_set(redis_name="",values=[],cur_redis=get_redis()):
    while len(values)>0:

        cur_redis.sadd(redis_name,values.pop(0))

    # Set 的 key
    # set_key = "c_value_contract_addr"

    # # 添加元素到 Set
    # r.sadd(set_key, "element1")

    # # 添加多元素
    # client.sadd(set_key, "element4", "element5")

    # # 获取所有元素
    # all_elements = client.smembers(set_key)
    # print("All elements in set:", all_elements)

    # # 检查元素是否存在
    # is_member = client.sismember(set_key, "element1")
    # print("Is 'element1' a member of the set?", is_member)

    # # 移除元素
    # client.srem(set_key, "element2")

    # # 检查移除后的元素
    # all_elements_after_removal = client.smembers(set_key)
    # print("All elements after removal:", all_elements_after_removal)

    # # 获取元素个数
    # size = client.scard(set_key)
    # print("Number of elements in the set:", size)


if __name__ =='__main__':
    # r=get_redis()
    # r.set("exac","me test")
    # print(r.get("exac"))
    redis_name="cex_big_transfer"
    
    is_in_set(redis_name="c_value_contract_addr",value="0xcee284f754e854890e311e3280b767f80797180d")

    
    # insert_token_holder(token_name="usdt")
    # insert_token_holder(token_name="usdt")
    # insert_token_holder(token_name="link",redis_ip="61.147.115.210")

    

    
    # for i in res:
    #     # print(i)
    #     lrange_one_json(redis_name=redis_name,one_json=i)
        # lrange_one_json(redis_name=redis_name,one_json=i)


    # print(r.llen(redis_name))
    # print(r.delete("cex_big_transfer_token_map_1"))
    # print(r.llen(redis_name))
    # print(r.lrange("look_on_chain_1025",0,100))
    


