package usrhandle;

import org.json.JSONException;
import org.json.JSONObject;
import tools.Constants;

import java.io.*;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.util.*;

/**
 * Created by tian on 16-1-4.
 */
public class SensitiveWord {
    public static JSONObject sensitiveWord;
    public static Map<String, String> sensitiveWordCorrespondId;
    public static Map<String, String> sensitiveWordCorrespondParents;
    private String sensitiveWord_get_url;

    private void init_sensitiveWord(){
        try {
            URL url = new URL(sensitiveWord_get_url);
            URLConnection urlcon = url.openConnection();
            InputStream is = urlcon.getInputStream();
            BufferedReader buffer = new BufferedReader(new InputStreamReader(is));
            String s = buffer.readLine();
            sensitiveWord = new JSONObject(s);
            buffer.close();
        } catch (JSONException e) {
            e.printStackTrace();
        } catch (MalformedURLException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static HashMap<String, List<String>> sensitiveWordDirectory;
    public static List<String> sensitiveWordList;
    public static List<String> sensitiveCategoryList;

    private void init_sensitiveWord_attribute(){
        Iterator iterator = sensitiveWord.keys();
        while (iterator.hasNext()) {
            String key = iterator.next().toString();
            JSONObject jsonObject = null;
            try {
                jsonObject = sensitiveWord.getJSONObject(key);
            } catch (JSONException e) {
                e.printStackTrace();
            }
            Iterator iterator1 = jsonObject.keys();
            List<String> list = new ArrayList<>();
            while (iterator1.hasNext()) {
                String key1 = iterator1.next().toString();
                String value = null;
                try {
                    value = jsonObject.getString(key1);
                } catch (JSONException e) {
                    e.printStackTrace();
                }
                list.add(key1 + "_" + value);
                sensitiveWordCorrespondId.put(key1, value);
                sensitiveWordList.add(key1);
                sensitiveWordCorrespondParents.put(key1, key);
            }
            sensitiveWordDirectory.put(key, list);
            sensitiveCategoryList.add(key);
        }
    }

    private boolean isNeedChange;

    public SensitiveWord(String sensitiveWord_get_url) {
        this.sensitiveWord_get_url = sensitiveWord_get_url;
        sensitiveWordCorrespondId = new HashMap<>();
        sensitiveWordCorrespondParents = new HashMap<>();
        sensitiveWordDirectory = new HashMap<>();
        sensitiveWordList = new ArrayList<>();
        sensitiveCategoryList = new ArrayList<>();
        init();
    }

    public void init(){
        init_sensitiveWord();
        init_sensitiveWord_attribute();
    }

    public static void main(String[] args) {
        System.out.println(SensitiveWord.sensitiveWordDirectory.toString());
        System.out.println(SensitiveWord.sensitiveWordList.toString());
        System.out.println(SensitiveWord.sensitiveCategoryList.toString());
        System.out.println(SensitiveWord.sensitiveWordCorrespondId.toString());
        System.out.println(SensitiveWord.sensitiveWordCorrespondParents.toString());
    }
}
