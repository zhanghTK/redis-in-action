package tk.zhangh.redis;

import org.junit.After;
import org.junit.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Tuple;

import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;

import static tk.zhangh.redis.Config.HOST;
import static tk.zhangh.redis.Config.PORT;

/**
 * Created by ZhangHao on 2017/9/27.
 */
public class Chapter02Test {
    private static final Jedis CONN = new Jedis(HOST, PORT);
    private static final Chapter02 APP = new Chapter02();

    @After
    public void clean() {
        CONN.flushAll();
    }

    @Test
    public void testLoginCookies() throws InterruptedException {
        System.out.println("\n----- testLoginCookies -----");
        String token = UUID.randomUUID().toString();

        APP.updateToken(CONN, token, "username", "itemX");
        System.out.println("We just logged-in/updated token: " + token);
        System.out.println("For user: 'username'\n");

        System.out.println("What username do we get when we look-up that token?");
        String r = APP.checkToken(CONN, token);
        System.out.println(r + "\n");
        assert r != null;

        System.out.println("Let's drop the maximum number of cookies to 0 to clean them out");
        System.out.println("We will start a thread to do the cleaning, while we stop it later");

        Chapter02.CleanSessionsThread thread = new Chapter02.CleanSessionsThread(new Jedis(HOST), 0);
        thread.start();
        Thread.sleep(1000);
        thread.quit();
        Thread.sleep(2000);
        if (thread.isAlive()) {
            throw new RuntimeException("The clean sessions thread is still alive?!?");
        }

        long s = CONN.hlen("login:");
        System.out.println("The current number of sessions still available is: " + s);
        assert s == 0;
    }

    @Test
    public void testShoppingCartCookies() throws InterruptedException {
        System.out.println("\n----- testShopppingCartCookies -----");
        String token = UUID.randomUUID().toString();

        System.out.println("We'll refresh our session...");
        APP.updateToken(CONN, token, "username", "itemX");
        System.out.println("And add an item to the shopping cart");
        APP.addToCart(CONN, token, "itemY", 3);
        Map<String, String> r = CONN.hgetAll("cart:" + token);
        System.out.println("Our shopping cart currently has:");
        for (Map.Entry<String, String> entry : r.entrySet()) {
            System.out.println("  " + entry.getKey() + ": " + entry.getValue());
        }

        assert r.size() >= 1;

        System.out.println("\nLet's clean out our sessions and carts");
        Chapter02.CleanFullSessionsThread thread = new Chapter02.CleanFullSessionsThread(new Jedis(HOST), 0);
        thread.start();
        Thread.sleep(1000);
        thread.quit();
        Thread.sleep(2000);
        if (thread.isAlive()) {
            throw new RuntimeException("The clean sessions thread is still alive?!?");
        }

        r = CONN.hgetAll("cart:" + token);
        System.out.println("Our shopping cart now contains:");
        for (Map.Entry<String, String> entry : r.entrySet()) {
            System.out.println("  " + entry.getKey() + ": " + entry.getValue());
        }
        assert r.size() == 0;
    }

    @Test
    public void testCacheRows() throws InterruptedException {
        System.out.println("\n----- testCacheRows -----");
        System.out.println("First, let's schedule caching of itemX every 5 seconds");
        APP.scheduleRowCache(CONN, "itemX", 5);
        System.out.println("Our schedule looks like:");
        Set<Tuple> s = CONN.zrangeWithScores("schedule:", 0, -1);
        for (Tuple tuple : s) {
            System.out.println("  " + tuple.getElement() + ", " + tuple.getScore());
        }
        assert s.size() != 0;

        System.out.println("We'll start a caching thread that will cache the data...");

        Chapter02.CacheRowsThread thread = new Chapter02.CacheRowsThread(new Jedis(HOST));
        thread.start();

        Thread.sleep(2000);
        System.out.println("Our cached data looks like:");
        String r = CONN.get("inv:itemX");
        System.out.println(r + "\n");
        assert r != null;

        System.out.println("We'll check again in 5 seconds...");
        Thread.sleep(5000);
        System.out.println("Notice that the data has changed...");
        String r2 = CONN.get("inv:itemX");
        System.out.println(r2 + "\n");
        assert r2 != null;
        assert !r.equals(r2);

        System.out.println("Let's force un-caching");
        APP.scheduleRowCache(CONN, "itemX", -1);
        Thread.sleep(1000);
        r = CONN.get("inv:itemX");
        System.out.println("The cache was cleared? " + (r == null));
        assert r == null;

        thread.quit();
        Thread.sleep(2000);
        if (thread.isAlive()) {
            throw new RuntimeException("The database caching thread is still alive?!?");
        }
    }

    @Test
    public void testCacheRequest() {
        System.out.println("\n----- testCacheRequest -----");
        String token = UUID.randomUUID().toString();

        Function<String, String> callback = s -> "content for " + s;

        APP.updateToken(CONN, token, "username", "itemX");
        String url = "http://test.com/?item=itemX";
        System.out.println("We are going to cache a simple request against " + url);
        String result = APP.cacheRequest(CONN, url, callback);
        System.out.println("We got initial content:\n" + result);
        System.out.println();

        assert result != null;

        System.out.println("To test that we've cached the request, we'll pass a bad callback");
        String result2 = APP.cacheRequest(CONN, url, null);
        System.out.println("We ended up getting the same response!\n" + result2);

        assert result.equals(result2);

        assert !APP.canCache(CONN, "http://test.com/");
        assert !APP.canCache(CONN, "http://test.com/?item=itemX&_=1234536");
    }

    @Test
    public void testRescaleViewed() throws InterruptedException {
        System.out.println("\n----- testRescaleViewed -----");
        String token = UUID.randomUUID().toString();

        for (int i = 0; i < 10; i++) {
            APP.updateToken(CONN, token, "username", "itemX");
        }
        double score = getScore("viewed:");
        System.out.println("Before RescaleViewedThread start, source:" + score);
        Chapter02.RescaleViewedThread rescaleViewedThread = new Chapter02.RescaleViewedThread(new Jedis(HOST));
        rescaleViewedThread.start();
        Thread.sleep(1000);
        double newSource = getScore("viewed:");
        System.out.println("After RescaleViewedThread start, source:" + newSource);
        assert newSource > score;
    }

    private double getScore(String key) {
        Set<Tuple> range = CONN.zrangeWithScores(key, 0, 0);
        Tuple next = range.size() > 0 ? range.iterator().next() : null;
        assert next != null;
        return next.getScore();
    }
}