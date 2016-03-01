package tools;

import java.io.*;
import java.util.*;

/**
 * Created by tian on 16-1-21.
 */
public class Rule {
    private static Set<String> URL_FOR_LEARN = new HashSet<String>() {
        {
        }
    };
    private static Set<String> URL_FOR_SCIENCE = new HashSet<String>() {
        {
        }
    };
    private static Set<String> URL_FOR_ENTERTAINMENT = new HashSet<String>() {
        {
        }
    };
    private static Set<String> URL_FOR_MILITARY = new HashSet<String>() {
        {
        }
    };
    private static Set<String> URL_FOR_SPORTS = new HashSet<String>() {
        {
        }
    };
    private static Set<String> URL_FOR_HEALTHY = new HashSet<String>() {
        {
        }
    };
    private static Set<String> URL_FOR_FINANCE = new HashSet<String>() {
        {
        }
    };
    private static Set<String> URL_FOR_SOCIOLOGY = new HashSet<String>() {
        {
        }
    };
    private static Set<String> URL_FOR_JOB = new HashSet<String>(){
        {

        }
    };

    private static Set<String> URL_FOR_COMPUTER = new HashSet<String>() {
        {
            add("csdn");
            add("www.bjsxt.com");
            add("api.datatang.com");
            add("developer.51cto.com");
        }
    };
    private static Set<String> URL_FOR_GAME = new HashSet<String>(){
        {

        }
    };

    static{
        URL_FOR_LEARN = initRule("/learn");
        URL_FOR_SPORTS = initRule("/sport");
        URL_FOR_HEALTHY = initRule("/healthy");
        URL_FOR_MILITARY = initRule("/military");
        URL_FOR_FINANCE = initRule("/finance");
        URL_FOR_JOB = initRule("/job");
        URL_FOR_SCIENCE = initRule("/science");
        URL_FOR_GAME = initRule("/game");
        URL_FOR_ENTERTAINMENT = initRule("/entertainment");
    };

    public final static Map<String, Set> RULE_FOR_LAYER_TWO = new HashMap<String, Set>() {
        {
            put("计算机",URL_FOR_COMPUTER);
            put("游戏",URL_FOR_GAME);
        }
    };

    public final static Map<String, Set> RULE_FOR_LAYER_ONE = new HashMap<String, Set>() {
        {
//            put("社会", URL_FOR_SOCIOLOGY);
//            put("财经", URL_FOR_FINANCE);
//            put("健康", URL_FOR_HEALTHY);
//            put("体育", URL_FOR_SPORTS);
//            put("军事", URL_FOR_MILITARY);
            put("娱乐", URL_FOR_ENTERTAINMENT);
//            put("科技", URL_FOR_SCIENCE);
            put("专业", URL_FOR_LEARN);
            put("工作", URL_FOR_JOB);
        }
    };

    private static Set<String> initRule(String filename){
        Set<String> set = new HashSet<>();
        try {
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(Rule.class.getResourceAsStream(filename)));
            String s;
            while((s = bufferedReader.readLine())!= null){
                if(!s.isEmpty())
                    set.add(s);
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return set;
    };

    public static void main(String[] args) {
        System.out.println(RULE_FOR_LAYER_ONE.size());
        for (Map.Entry<String, Set> entry : RULE_FOR_LAYER_ONE.entrySet()) {
            Set<String> set = entry.getValue();
            for (String s : set) {
                System.out.println(entry.getKey() + "f  f" + s);
            }
        }
    }
}
