package tk.zhangh.redis;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Transaction;

import java.util.List;

/**
 * Created by ZhangHao on 2017/9/28.
 */
public class Chapter04 {
    /*======================================= 商品交易 ====================================================================
    1. 用户信息（users：ID）
    2. 用户包裹（inventory：ID）
    3. 市场（market）
    */

    /**
     * 销售商品
     */
    public boolean listItem(Jedis conn, String itemId, String sellerId, double price) {
        String inventory = "inventory:" + sellerId;
        String item = itemId + '.' + sellerId;
        long end = System.currentTimeMillis() + 5000;

        while (System.currentTimeMillis() < end) {
            conn.watch(inventory);  // 监视包裹
            if (!conn.sismember(inventory, itemId)) {
                // 指定商品不存在用户的包裹
                conn.unwatch();
                return false;
            }
            // 事务执行
            Transaction trans = conn.multi();
            trans.zadd("market:", price, item);
            trans.srem(inventory, itemId);
            List<Object> results = trans.exec();
            if (results == null) {
                continue;
            }
            return true;
        }
        return false;
    }

    /**
     * 购买商品
     */
    public boolean purchaseItem(Jedis conn, String buyerId, String itemId, String sellerId, double lprice) {
        String buyer = "users:" + buyerId;
        String seller = "users:" + sellerId;
        String item = itemId + "." + sellerId;
        String inventory = "inventory:" + buyerId;
        long end = System.currentTimeMillis() + 10000;

        while (System.currentTimeMillis() < end) {
            conn.watch("market:", buyer);
            double price = conn.zscore("market:", item);
            double funds = Double.parseDouble(conn.hget(buyer, "funds"));
            if (price != lprice || // 商品价钱变化
                    price > funds) {  // 余额不足
                conn.unwatch();
                return false;
            }

            // 执行事务
            Transaction trans = conn.multi();
            trans.hincrBy(seller, "funds", ((int) price));  // 卖家加钱
            trans.hincrBy(buyer, "funds", ((int) -price));  // 买家扣钱
            trans.sadd(inventory, itemId);  // 买家添加商品
            trans.zrem("market:", item);  // 市场移除商品
            List<Object> results = trans.exec();
            if (results == null) {
                continue;
            }
            return true;
        }
        return false;
    }


    /*======================================= Chapter02.updateToken改进 ================================================
    使用非事务流水线，使用 sync 方法提交，而不是 exec，这点需要注意
    */

    public void updateTokenPipeline(Jedis conn, String token, String user, String... item) {
        long timestamp = System.currentTimeMillis() / 1000;
        Pipeline pipe = conn.pipelined();
        pipe.hset("login:", token, user);
        pipe.zadd("recent:", timestamp, token);
        if (item != null) {
            pipe.zadd("viewed:" + token, timestamp, item[0]);
            pipe.zremrangeByRank("viewed:" + token, 0, -26);
            pipe.zincrby("viewed:", -1, item[0]);
        }
        pipe.sync();
    }
}
