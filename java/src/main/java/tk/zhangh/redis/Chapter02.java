package tk.zhangh.redis;

import com.alibaba.fastjson.JSON;
import lombok.AllArgsConstructor;
import lombok.Data;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Tuple;
import redis.clients.jedis.ZParams;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.*;
import java.util.function.Function;

/**
 * Created by ZhangHao on 2017/9/27.
 */
public class Chapter02 {

    /*=======================================购物网站Web应用====================================================================
    使用9个缓存结构：
    1. 登录令牌缓存（login:）
    2. 最近访问令牌缓存（recent:）
    3. 用户最近访问商品缓存（viewed:TOKEN）
    4. 购物车缓存（cart:SESSION）
    5. 商品访问统计缓存（viewed:）
    6. 页面缓存（cache:PAGE_KEY）
    7. 数据行延时缓存（delay:）
    8. 数据行调度时刻缓存（schedule）
    9. 数据行缓存（inv:ROW_ID）
     */


    /**
     * 根据token查找用户
     */
    public String checkToken(Jedis conn, String token) {
        return conn.hget("login:", token);
    }

    /**
     * 更新token
     */
    public void updateToken(Jedis conn, String token, String user, String... item) {
        long timestamp = System.currentTimeMillis() / 1000;
        conn.hset("login:", token, user);  // 登录令牌
        conn.zadd("recent:", timestamp, token);  // 最近访问令牌缓存添加记录
        if (item != null) {
            // 访问了具体商品
            conn.zadd("viewed:" + token, timestamp, item[0]);  // 用户最近访问商品缓存添加记录
            conn.zremrangeByRank("viewed:" + token, 0, -26);  // 保留最近的25个
            conn.zincrby("viewed:", -1, item[0]);  // 每预览一次商品 score-1，保证浏览最多的商品在有序集合最前端
        }
    }

    /**
     * 清理会话守护线程
     */
    @AllArgsConstructor
    public static class CleanSessionsThread extends DaemonThread {
        private Jedis conn;
        private int limit = 1000;  // 保留的最大会话数

        @Override
        public void run() {
            while (!quit) {
                long size = conn.zcard("recent:");  // 最近登录用户数
                if (size <= limit) {
                    // 最近登录用户数小于最大会话数，跳过
                    try {
                        sleep(1000);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    continue;
                }

                long endIndex = Math.min(size - limit, 100);
                // 需要清理的token
                Set<String> tokenSet = conn.zrange("recent:", 0, endIndex - 1);
                String[] tokens = tokenSet.toArray(new String[tokenSet.size()]);
                // 拼接用户最近访问商品缓存的key
                List<String> sessionKeys = new ArrayList<String>();
                for (String token : tokens) {
                    sessionKeys.add("viewed:" + token);
                }
                // 清理缓存
                conn.del(sessionKeys.toArray(new String[sessionKeys.size()]));  // 移除用户最近访问商品缓存（viewed:TOKEN）
                conn.hdel("login:", tokens);  // 移除登录令牌缓存（login：）
                conn.zrem("recent:", tokens);  // 移除最近访问令牌（recent:）
            }
        }
    }

    /**
     * 添加商品到购物车
     */
    public void addToCart(Jedis conn, String session, String item, int count) {
        if (count <= 0) {
            conn.hdel("cart:" + session, item);
        } else {
            conn.hset("cart:" + session, item, String.valueOf(conn));
        }
    }

    /**
     * 清理会话守护线程
     * 增加清理购物车的能力
     */
    @AllArgsConstructor
    public static class CleanFullSessionsThread extends DaemonThread {
        private Jedis conn;
        private int limit;

        @Override
        public void run() {
            while (!quit) {
                long size = conn.zcard("recent:");  // 最近登录用户数
                if (size <= limit) {
                    try {
                        sleep(1000);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    continue;
                }

                long endIndex = Math.min(size - limit, 100);
                // 需要清理的token
                Set<String> tokenSet = conn.zrange("recent:", 0, endIndex - 1);
                String[] tokens = tokenSet.toArray(new String[tokenSet.size()]);
                // 拼接用户最近访问商品缓存的key，购物车缓存的key
                List<String> sessionKeys = new ArrayList<String>();
                for (String token : tokens) {
                    sessionKeys.add("viewed:" + token);
                    sessionKeys.add("cart:" + token);
                }

                // 移除最近访问商品缓存（viewed:TOKEN）,购物车缓存（cart:TOKEN）
                conn.del(sessionKeys.toArray(new String[sessionKeys.size()]));
                conn.hdel("login:", tokens);  // 移除登录令牌缓存（login：）
                conn.zrem("recent:", tokens);  // 移除最近访问令牌（recent:）
            }
        }
    }

    /**
     * 缓存请求对应
     */
    public String cacheRequest(Jedis conn, String request, Function<String, String> callback) {
        if (!canCache(conn, request)) {
            return callback != null ? callback.apply(request) : null;
        }

        String pageKey = "cache:" + hashRequest(request);
        String content = conn.get(pageKey);

        if (content == null && callback != null) {
            content = callback.apply(request);
            conn.setex(pageKey, 300, content);
        }

        return content;
    }

    /**
     * 添加数据行时间调度缓存记录
     */
    public void scheduleRowCache(Jedis conn, String rowId, int delay) {
        conn.zadd("delay:", delay, rowId);  // 数据行的延迟值缓存添加记录
        conn.zadd("schedule:", System.currentTimeMillis() / 1000, rowId);  // 数据行调度时刻缓存添加记录
    }

    /**
     * 数据行缓存定时更新任务
     */
    @AllArgsConstructor
    public static class CacheRowsThread extends DaemonThread {
        private Jedis conn;

        @Override
        public void run() {
            while (!quit) {
                // 将要执行的定时任务
                Set<Tuple> range = conn.zrangeWithScores("schedule:", 0, 0);
                Tuple next = range.size() > 0 ? range.iterator().next() : null;
                long now = System.currentTimeMillis() / 1000;
                if (next == null || next.getScore() > now) {
                    // 没有符合条件的任务
                    try {
                        sleep(50);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    continue;
                }
                String rowId = next.getElement();
                double delay = conn.zscore("delay:", rowId);  // 下次执行时间
                if (delay <= 0) {
                    // 移除定时任务
                    conn.zrem("delay:", rowId);
                    conn.zrem("schedule:", rowId);
                    conn.del("inv:" + rowId);
                    continue;
                }

                Inventory row = Inventory.get(rowId);  // 模拟更新数据
                conn.zadd("schedule:", now + delay, rowId);
                conn.set("inv:" + rowId, JSON.toJSONString(row));
            }
        }
    }

    @AllArgsConstructor
    public static class RescaleViewedThread extends DaemonThread {
        private Jedis conn;

        @Override
        public void run() {
            while (!quit) {
                conn.zremrangeByRank("viewed:", 0, -20001);  // 只保留前20000
                ZParams params = new ZParams().weightsByDouble(0.5);
                conn.zinterstore("viewed:", params, "viewed:");  // 保留为原来一般
                try {
                    sleep(500);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    public boolean canCache(Jedis conn, String request) {
        try {
            // 获取参数
            URL url = new URL(request);
            HashMap<String, String> params = new HashMap<>();
            if (url.getQuery() != null) {
                for (String param : url.getQuery().split("&")) {
                    String[] pair = param.split("=", 2);
                    params.put(pair[0], pair.length == 2 ? pair[1] : null);
                }
            }
            String itemId = extractItemId(params);
            if (itemId == null || isDynamic(params)) {
                // itemId 不为空，不存在动态参数
                return false;
            }
            Long rank = conn.zrank("viewed:", itemId);
            return rank != null && rank < 10000;  // 限制只缓存最经常预览的1k个商品
        } catch (MalformedURLException mue) {
            return false;
        }
    }

    public String extractItemId(Map<String, String> params) {
        return params.get("item");
    }

    public boolean isDynamic(Map<String, String> params) {
        return params.containsKey("_");
    }

    public String hashRequest(String request) {
        return String.valueOf(request.hashCode());
    }

    @AllArgsConstructor
    @Data
    public static class Inventory {
        private String id;
        private String data;
        private long time;

        public static Inventory get(String id) {
            return new Inventory(id, "data to cache...", System.currentTimeMillis() / 1000);
        }
    }

    public static class DaemonThread extends Thread {
        protected volatile boolean quit;

        public void quit() {
            quit = true;
        }
    }
}
