package tk.zhangh.redis;

import redis.clients.jedis.*;

import java.util.Arrays;
import java.util.List;

import static tk.zhangh.redis.Config.HOST;
import static tk.zhangh.redis.Config.PORT;

/**
 * Jedis 调用方式汇总
 * Created by ZhangHao on 2017/9/28.
 */
public class JedisCall {

    private static final long TIMES = 1000;
    private static final Jedis JEDIS;
    private static final ShardedJedis SHARDED_JEDIS;
    private static final ShardedJedisPool SHARDED_JEDIS_POOL;

    static {
        List<JedisShardInfo> shards = Arrays.asList(
                new JedisShardInfo(HOST, PORT),
                new JedisShardInfo(HOST, PORT));


        JEDIS = new Jedis(HOST, PORT);
        SHARDED_JEDIS = new ShardedJedis(shards);

        SHARDED_JEDIS_POOL = new ShardedJedisPool(new JedisPoolConfig(), shards);
    }

    /**
     * 普通调用
     */
    public void normal() {
        long start = System.currentTimeMillis();
        for (int i = 0; i < TIMES; i++) {
            JEDIS.set("n" + i, "n" + i);
        }
        long end = System.currentTimeMillis();
        System.out.println("Simple SET: " + ((end - start) / 1000.0) + " seconds");
    }

    /**
     * 事务调用
     */
    public void trans() {
        long start = System.currentTimeMillis();
        Transaction tx = JEDIS.multi();
        for (int i = 0; i < TIMES; i++) {
            tx.set("t" + i, "t" + i);
        }
        tx.exec();
        long end = System.currentTimeMillis();
        System.out.println("Transaction SET: " + ((end - start) / 1000.0) + " seconds");
    }

    /**
     * 管道
     */
    public void pipelined() {
        long start = System.currentTimeMillis();
        Pipeline pipeline = JEDIS.pipelined();
        for (int i = 0; i < TIMES; i++) {
            pipeline.set("p" + i, "p" + i);
        }
        pipeline.syncAndReturnAll();
        long end = System.currentTimeMillis();
        System.out.println("Pipelined SET: " + ((end - start) / 1000.0) + " seconds");
    }

    /**
     * 管道中使用事务
     */
    public void combPipelineTrans() {
        long start = System.currentTimeMillis();
        Pipeline pipeline = JEDIS.pipelined();
        pipeline.multi();
        for (int i = 0; i < TIMES; i++) {
            pipeline.set("PT" + i, "PT" + i);
        }
        pipeline.exec();  // 事务
        pipeline.syncAndReturnAll();  // 管道
        long end = System.currentTimeMillis();
        System.out.println("Pipelined transaction: " + ((end - start) / 1000.0) + " seconds");
    }

    /**
     * 分布式直连同步调用
     */
    public void shardNormal() {
        long start = System.currentTimeMillis();
        for (int i = 0; i < TIMES; i++) {
            SHARDED_JEDIS.set("sn" + i, "sn" + i);
        }
        long end = System.currentTimeMillis();
        System.out.println("Simple Sharing SET: " + ((end - start) / 1000.0) + " seconds");
    }

    /**
     * 分布式直连异步调用
     */
    public void shardPipelined() {
        long start = System.currentTimeMillis();
        ShardedJedisPipeline pipeline = SHARDED_JEDIS.pipelined();
        for (int i = 0; i < TIMES; i++) {
            pipeline.set("sp" + i, "p" + i);
        }
        pipeline.syncAndReturnAll();
        long end = System.currentTimeMillis();
        System.out.println("Pipelined Sharing SET: " + ((end - start) / 1000.0) + " seconds");
    }

    /**
     * 分布式连接池同步调用
     */
    public void shardSimplePool() {
        long start = System.currentTimeMillis();
        ShardedJedis one = SHARDED_JEDIS_POOL.getResource();
        for (int i = 0; i < TIMES; i++) {
            one.set("spn" + i, "n" + i);
        }
        long end = System.currentTimeMillis();
        one.close();
        System.out.println("Simple Pool SET: " + ((end - start)/1000.0) + " seconds");
    }

    /**
     * 分布式连接池异步调用
     */
    public void shardPipelinedPool() {
        long start = System.currentTimeMillis();
        ShardedJedis one = SHARDED_JEDIS_POOL.getResource();
        ShardedJedisPipeline pipeline = one.pipelined();

        for (int i = 0; i < TIMES; i++) {
            pipeline.set("sppn" + i, "n" + i);
        }
        pipeline.syncAndReturnAll();
        long end = System.currentTimeMillis();
        one.close();
        System.out.println("Pipelined Pool SET: " + ((end - start)/1000.0) + " seconds");
    }

    public static void main(String[] args) {
        JedisCall jedisCall = new JedisCall();
        jedisCall.normal();
        jedisCall.trans();
        jedisCall.pipelined();
        jedisCall.combPipelineTrans();
        jedisCall.shardNormal();
        jedisCall.shardPipelined();
        jedisCall.shardSimplePool();
        jedisCall.shardPipelinedPool();
    }
}
