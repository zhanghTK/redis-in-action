package tk.zhangh.redis;

import org.junit.After;
import org.junit.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Tuple;

import java.util.Map;
import java.util.Set;

import static tk.zhangh.redis.Config.HOST;
import static tk.zhangh.redis.Config.PORT;

/**
 * Created by ZhangHao on 2017/9/28.
 */
public class Chapter04Test {

    private static final Jedis CONN = new Jedis(HOST, PORT);
    private static final Chapter04 APP = new Chapter04();

    @After
    public void clean() {
        CONN.flushAll();
    }

    @Test
    public void listItem() throws Exception {
        System.out.println("\n----- testListItem -----");
        listItem0();
    }

    private void listItem0() throws Exception {
        System.out.println("We need to set up just enough state so that a user can list an item");
        String seller = "userX";
        String item = "itemX";
        CONN.sadd("inventory:" + seller, item);
        Set<String> i = CONN.smembers("inventory:" + seller);

        System.out.println("The user's inventory has:");
        for (String member : i) {
            System.out.println("  " + member);
        }
        assert i.size() > 0;

        System.out.println("\nListing the item...");
        boolean l = APP.listItem(CONN, item, seller, 10);
        System.out.println("Listing the item succeeded? " + l);
        assert l;
        Set<Tuple> r = CONN.zrangeWithScores("market:", 0, -1);
        System.out.println("The market contains:");
        for (Tuple tuple : r) {
            System.out.println("  " + tuple.getElement() + ", " + tuple.getScore());
        }
        assert r.size() > 0;
    }

    @Test
    public void purchaseItem() throws Exception {
        System.out.println("\n----- testPurchaseItem -----");
        listItem0();

        System.out.println("\nWe need to set up just enough state so a user can buy an item");
        CONN.hset("users:userY", "funds", "125");
        Map<String, String> r = CONN.hgetAll("users:userY");
        System.out.println("The user has some money:");
        for (Map.Entry<String, String> entry : r.entrySet()) {
            System.out.println("  " + entry.getKey() + ": " + entry.getValue());
        }
        assert r.size() > 0;
        assert r.get("funds") != null;

        System.out.println("\nLet's purchase an item");
        boolean p = APP.purchaseItem(CONN, "userY", "itemX", "userX", 10);
        System.out.println("Purchasing an item succeeded? " + p);
        assert p;
        r = CONN.hgetAll("users:userY");
        System.out.println("Their money is now:");
        for (Map.Entry<String, String> entry : r.entrySet()) {
            System.out.println("  " + entry.getKey() + ": " + entry.getValue());
        }
        assert r.size() > 0;

        String buyer = "userY";
        Set<String> i = CONN.smembers("inventory:" + buyer);
        System.out.println("Their inventory is now:");
        for (String member : i) {
            System.out.println("  " + member);
        }
        assert i.size() > 0;
        assert i.contains("itemX");
        assert CONN.zscore("market:", "itemX.userX") == null;
    }
}