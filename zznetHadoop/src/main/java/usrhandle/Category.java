package usrhandle;

import org.json.JSONException;
import org.json.JSONObject;
import tools.Constants;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.util.*;

/**
 * Created by tian on 15-12-18.
 */

public class Category {
    public static JSONObject category;
    public static Map<String, String> categoryCorrespondId = null;
    public static Map<String, String> categorySuperId = null;
    public static Set<String> categorySuper = null;
    public static Map<String, String> superCategoryId = null;
    public static String learn = "学习";
    private String category_get_url;

    private void init_category() {
        try {
            URL url = new URL(category_get_url);
            URLConnection urlcon = url.openConnection();
            InputStream is = urlcon.getInputStream();
            BufferedReader buffer = new BufferedReader(new InputStreamReader(is));
            String s = buffer.readLine();
            category = new JSONObject(s);
            buffer.close();
        } catch (JSONException e) {
            e.printStackTrace();
        } catch (MalformedURLException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void init_category_attribute() {
        Iterator iterator = category.keys();
        while (iterator.hasNext()) {
            String key = iterator.next().toString();
            JSONObject jsonObject = null;
            String[] splits = key.split(Constants.CATEGORY_ID_SPLIT);
            categorySuper.add(splits[0]);
            if (learn.equals(splits[0]))
                learn = key;
//            System.out.println(learn);
            superCategoryId.put(splits[0], splits[1]);
            try {
                jsonObject = category.getJSONObject(key);
            } catch (JSONException e) {
                e.printStackTrace();
            }
            Iterator iterator1 = jsonObject.keys();
            while (iterator1.hasNext()) {
                String key1 = iterator1.next().toString();
                String value = null;
                try {
                    value = jsonObject.getString(key1);
                    categoryCorrespondId.put(key1, value);
                    categorySuperId.put(key1, splits[1]);
                } catch (JSONException e) {
                    e.printStackTrace();
                } catch (Exception e) {
                    e.printStackTrace();
                }

            }
        }
    }

    private boolean isNeedChange;

    public Category(String category_get_url) {
        this.category_get_url = category_get_url;
        categoryCorrespondId = new HashMap<>();
        categorySuperId = new HashMap<>();
        categorySuper = new HashSet<>();
        superCategoryId = new HashMap<>();
        init();
    }

    public void init(){
        init_category();
        init_category_attribute();
    }

    private double similarScore(String word, String type) throws IOException, InterruptedException {
        double score;
        URL url = new URL(String.format("http://192.168.1.106:8080/zz_nlp/wordsDistance?word1=%s&word2=%s", word, type));
        URLConnection urlcon = url.openConnection();
        InputStream is = urlcon.getInputStream();
        BufferedReader buffer = new BufferedReader(new InputStreamReader(is));
        String s = buffer.readLine();
        score = Double.valueOf(s);
        buffer.close();
        return score;
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        Category category1 = new Category("http://192.168.1.106:8080/public_behavior/api/behavior.do");
        category1.init();
        for (Map.Entry<String, String> entry : categorySuperId.entrySet()) {
            if(entry.getValue().toString().equals("1"))
                System.out.println("key= " + entry.getKey() + category1.similarScore(entry.getKey(), "专业")+"  "+category1.similarScore(entry.getKey(), "工作")+"  "+category1.similarScore(entry.getKey(), "娱乐"));
        }
        System.out.println();
        System.out.println();
        for (Map.Entry<String, String> entry : categorySuperId.entrySet()) {
            if(entry.getValue().toString().equals("5"))
                System.out.println("key= " + entry.getKey() + category1.similarScore(entry.getKey(), "专业")+"  "+category1.similarScore(entry.getKey(), "工作")+"  "+category1.similarScore(entry.getKey(), "娱乐"));
        }
        System.out.println();
        System.out.println();
        for (Map.Entry<String, String> entry : categorySuperId.entrySet()) {
            if(entry.getValue().toString().equals("10"))
                System.out.println("key= " + entry.getKey() + category1.similarScore(entry.getKey(), "专业")+"  "+category1.similarScore(entry.getKey(), "工作")+"  "+category1.similarScore(entry.getKey(), "娱乐"));
        }
        System.out.println(categoryCorrespondId);
        System.out.println(categorySuperId);
        System.out.println(categorySuper);
        System.out.println(superCategoryId);
    }
}
