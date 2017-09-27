package tk.zhangh.redis;

import org.junit.Test;
import redis.clients.jedis.Jedis;

import static tk.zhangh.redis.Config.HOST;
import static tk.zhangh.redis.Config.PORT;

/**
 * Created by ZhangHao on 2017/9/26.
 */
public class Chapter01Test {

    private static final Chapter01 APP = new Chapter01();
    private static final Jedis JEDIS = new Jedis(HOST, PORT);

    private static final String USER = "zh-user";
    private static final String ARTICLE = "zh-article";
    private static final String LINK = "zh-link";
    private static final String GROUP = "zh-group";

    @Test
    public void articleVote() throws Exception {
        APP.articleVote(JEDIS, USER, ARTICLE);
    }

    @Test
    public void postArticle() throws Exception {
        APP.postArticle(JEDIS, USER, ARTICLE, LINK);
    }

    @Test
    public void getArticles() throws Exception {
        APP.getArticles(JEDIS, 1);
    }

    @Test
    public void getArticles1() throws Exception {
        APP.getArticles(JEDIS, 1, "time:");
    }

    @Test
    public void addGroups() throws Exception {
        APP.addGroups(JEDIS, ARTICLE, new String[]{GROUP});
    }

    @Test
    public void getGroupArticles() throws Exception {
        APP.getGroupArticles(JEDIS, GROUP, 1);
    }

    @Test
    public void getGroupArticles1() throws Exception {
        APP.getGroupArticles(JEDIS, GROUP, 1, GROUP);
    }

}