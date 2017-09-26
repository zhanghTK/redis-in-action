package tk.zhangh.redis;


import redis.clients.jedis.Jedis;
import redis.clients.jedis.ZParams;

import java.util.*;

/**
 * Created by ZhangHao on 2017/9/26.
 */
public class Chapter01 {
    private static final int ONE_WEEK_IN_SECONDS = 7 * 86400;
    private static final int VOTE_SCORE = 432;
    private static final int ARTICLES_PER_PAGE = 25;

    /*=======================================文章投票====================================================================
    使用5个缓存结构：
    1. 文章Id缓存(article:)，字符串
    2. 文章缓存（article:ARTICLE_ID），哈希
    3. 文章投票人缓存(article:ARTICLE_ID)，集合
    4. 文章分数缓存(score:)，有序集合
    5. 文章发布时间缓存(time:)，有序集合
    6. 文章分组缓存(group:GROUP)，集合
    7. 时间排序分组缓存(time:GROUP)，有序集合
    8. 分数排序分组缓存（score:GROUP）,有序集合
     */

    /**
     * 投票
     */
    public void articleVote(Jedis conn, String user, String article) {
        long cutoff = (System.currentTimeMillis() / 1000) - ONE_WEEK_IN_SECONDS;  // 超过一周禁止投票
        if (conn.zscore("time:", article) == null ||conn.zscore("time:", article) < cutoff) {
            return;
        }

        String articleId = article.substring(article.indexOf(':') + 1);
        if (conn.sadd("voted:" + articleId, user) == 1) {
            // 第一次投票
            conn.zincrby("score:", VOTE_SCORE, article);
            conn.hincrBy(article, "votes", 1L);
        }
    }

    /**
     * 发布文章
     */
    public String postArticle(Jedis conn, String user, String title, String link) {
        String articleId = String.valueOf(conn.incr("article:"));  // 新的文章Id，递增

        String voted = "voted:" + articleId;
        conn.sadd(voted, user);  // 添加文章投票人缓存
        conn.expire(voted, ONE_WEEK_IN_SECONDS);  // 文章投票人缓存一周后过期

        // 添加文章缓存
        long now = System.currentTimeMillis() / 1000;
        String article = "article:" + articleId;
        HashMap<String, String> articleData = new HashMap<String, String>();
        articleData.put("title", title);
        articleData.put("link", link);
        articleData.put("user", user);
        articleData.put("now", String.valueOf(now));
        articleData.put("votes", "1");
        conn.hmset(article, articleData);
        // 添加文章分数缓存
        conn.zadd("score:", now + VOTE_SCORE, article);
        // 添加文章发布时间缓存
        conn.zadd("time:", now, article);

        return articleId;
    }

    /**
     * 根据分数分页获取文章
     */
    public List<Map<String, String>> getArticles(Jedis conn, int page) {
        return getArticles(conn, page, "score:");
    }

    /**
     * 分页获取文章
     */
    public List<Map<String, String>> getArticles(Jedis conn, int page, String order) {
        // 分页
        int start = (page - 1) * ARTICLES_PER_PAGE;
        int end = start + ARTICLES_PER_PAGE + 1;

        Set<String> ids = conn.zrange(order, start, end);
        List<Map<String, String>> articles = new ArrayList<Map<String, String>>();
        for (String id : ids) {
            Map<String, String> articleData = conn.hgetAll(id);
            articleData.put("id", id);
            articles.add(articleData);
        }
        return articles;
    }

    /**
     * 添加分组
     */
    public void addGroups(Jedis conn, String articleId, String[] toAdd) {
        String article = "article:" + articleId;
        for (String group : toAdd) {
            conn.sadd("group:" + group, article);
        }
    }

    /**
     * 获取分组文章，根据分数排序分页获得
     * @param conn
     * @param group
     * @param page
     * @return
     */
    public List<Map<String, String>> getGroupArticles(Jedis conn, String group, int page) {
        return getGroupArticles(conn, group, page, "score:");
    }

    /**
     * 获得分组的文章，根据指定排序分页获得
     */
    public List<Map<String, String>> getGroupArticles(Jedis conn, String group, int page, String order) {
        String key = order + group;  // 时间/分数排序分组缓存
        if (!conn.exists(key)) {
            // 创建临时排序分组
            ZParams params = new ZParams().aggregate(ZParams.Aggregate.MAX);
            conn.zinterstore(key, params, "group:" + group, order);
            conn.expire(key, 60);
        }
        return getArticles(conn, page, key);
    }

    public static void main(String[] args) {
        Chapter01 app = new Chapter01();
        app.postArticle(new Jedis("zh-home.tk"), "", "", "");
    }
}
