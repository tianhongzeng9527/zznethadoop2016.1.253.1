package usrhandle;

import org.json.JSONException;
import org.json.JSONObject;
import tools.Constants;
import tools.Rule;
import urlhandle.TitleKeyWords;

import java.io.*;
import java.net.URL;
import java.net.URLConnection;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by tian on 15-12-17.
 */
/*
1请求日期Datetime发起请求的时间,格式：20151217101232
2响应时间Datetime返回响应的时间,格式：20151217101232
3源IPString发起请求的客户端IP
4源端口String发起请求的客户端端口
5目的IPString服务器端IP
6目的端口String服务器端端口
7响应IPString返回响应到客户端的IP
8响应端口String返回响应到客户端端口
9客户端IPString接受响应的客户端IP
10客户端端口String接受响应的客户端端口
11请求方法String请求的方法类型，如GET
12URLString请求的URL
13User-AgentString用户的访问设备类型
14ReferURLString发起请求的上级URL
15CookieString发送请求时携带的Cookie集合，以Key=value形式
16域名String返回响应的
17URIString响应的URI18ContentString页面的HTML内容
 */
public class UserDataInformation {
    private String reqTime;
    private String reqHour;
    private String respTime;
    private String reqIP;
    private String reqPort;
    private String destinationIP;
    private String destinationPost;
    private String respIP;
    private String respPort;
    private String acceptRespIP;
    private String acceptRespPost;
    private String reqMethod;
    private String reqURL;
    private String userEquipment;
    private String reqSupURL;
    private String reqCookie;
    private String domainName;
    private String htmlContent;
    private String schoolMessage;
    private String referURL;
    private List<String> keyWords;
    public boolean isNormalMessage;
    private Type type;
    private TitleKeyWords titleKeyWords;
    private Set<String> containsSensitiveWordsList;
    private String normalCategory;
    private String sensitiveCategory;
    private String uid;
    private String uname;
    private String mac;
    private String categoryId;
    private List<String> sensitiveId = new ArrayList<>();
    private boolean isRightTime;
//    private String learnCategoryName = "";
    private String calculate_word_distance_url;

    public List<String> getSensitiveId() {
        return sensitiveId;
    }

    private void sensitiveId() {
        sensitiveId = new ArrayList<>();
        for (String sensitiveWord : containsSensitiveWordsList) {
            String id = SensitiveWord.sensitiveWordCorrespondId.get(sensitiveWord);
            sensitiveId.add(id);
        }
    }

    public UserDataInformation(String userDataInformation, String calculate_word_distance_url) throws IOException, ParseException {
        this.calculate_word_distance_url = calculate_word_distance_url;
        String[] splits = userDataInformation.trim().split(Constants.DECOLLATOR_FOR_HDFS_MESSAGE);
        isRightTime = true;
        normalCategory = Constants.NOISE;
        isNormalMessage = true;
        initUserInformation(splits);
        if (isRightTime()) {
            initKeyWords();
            containsSensitiveWordsList = titleKeyWords.getContainsSensitiveWordsList();
            sensitiveCategory = titleKeyWords.getSensitiveCategory();
//            System.out.println(titleKeyWords.getClaenHtml());
        }
    }

    public UserDataInformation(String userDataInformation) throws IOException, ParseException {
        String[] splits = userDataInformation.trim().split(Constants.DECOLLATOR_FOR_HDFS_MESSAGE);
        isRightTime = true;
        normalCategory = Constants.NOISE;
        isNormalMessage = true;
        initUserInformation(splits);
        if (isRightTime()) {
            initKeyWords();
            containsSensitiveWordsList = titleKeyWords.getContainsSensitiveWordsList();
            sensitiveCategory = titleKeyWords.getSensitiveCategory();
//            System.out.println(titleKeyWords.getClaenHtml());
        }
    }

    private void initUserInformation(String[] splits) throws ParseException {
        isRightTime = true;
        SimpleDateFormat df = new SimpleDateFormat("yyyyMMdd");
        SimpleDateFormat df2 = new SimpleDateFormat("yyyy-MM-dd");
        Date dt = df.parse(splits[0].substring(0, 8));
        reqTime = df2.format(dt);
        reqHour = splits[0].substring(8, 10);
        reqIP = splits[1];
        reqPort = splits[2];
        respIP = splits[3];
        respPort = splits[4];
        reqMethod = splits[6];
        reqURL = splits[5];
        int length = splits.length;
        if (length > 10)
            uid = splits[10];
        if (length > 11)
        if (length > 11)
            uname = splits[11];
        if (length > 12)
            mac = splits[12];
        if (length <= 10) {
            uid = "110";
            uname = "uname";
            mac = "mac";
        }
    }

    private void initKeyWords() throws IOException {
        titleKeyWords = new TitleKeyWords(new URL(reqURL), Constants.KEY_NUM);
        titleKeyWords.init();
        keyWords = titleKeyWords.getTopNumKey();
    }

    public void sensitiveClassify() {
        if (normalCategory == Constants.NOISE)
            if (containsSensitiveWordsList.size() >= Constants.SENSITIVE_WORD_NUM) {
                sensitiveId();
                this.type = Type.sensitive;
            }
    }

    public void calculateAndGenerateNormalCategory(Map<String, Integer> keyWordsType, Map<String, Double> wordsScore) {
        int max = 1;
        for (Map.Entry<String, Integer> entry : keyWordsType.entrySet()) {
            if (entry.getValue() > max) {
                normalCategory = entry.getKey();
                max = entry.getValue();
            }
        }
        if (max == 1) {
            double score = 0;
            for (Map.Entry<String, Double> entry : wordsScore.entrySet()) {
                if (entry.getValue() > score) {
                    normalCategory = entry.getKey();
                    score = entry.getValue();
                }
            }
        }
        keyWordsType.clear();
    }

    private void calculateAndGenerateKeyWordCategory(int i, JSONObject categoryJsonObject, Map<String, Integer> keyWordsType, Map<String, Double> wordsScore) throws IOException, InterruptedException {
        for (String keyWord : keyWords) {
            double maxScore = 0;
            Iterator iterate = categoryJsonObject.keys();
            while (iterate.hasNext()) {
                String tempCategory = (String) iterate.next();
//                if (!tempCategory.contains(Constants.LEARNING)) {
//                    double score = similarScore(keyWord, tempCategory.split(Constants.CATEGORY_ID_SPLIT)[0]);
//                    maxScore = calculateMaxScore(i, score, tempCategory, maxScore);
//                } else {
//                    learnCategoryName = tempCategory;
//                }
//                System.out.println(tempCategory.split(Constants.CATEGORY_ID_SPLIT)[0]);
                    double score = similarScore(keyWord, tempCategory.split(Constants.CATEGORY_ID_SPLIT)[0]);
                    maxScore = calculateMaxScore(i, score, tempCategory, maxScore);
            }
            if (!normalCategory.equals(Constants.NOISE)) {
                if (keyWordsType.containsKey(normalCategory))
                    keyWordsType.put(normalCategory, keyWordsType.get(normalCategory) + 1);
                else
                    keyWordsType.put(normalCategory, 1);
                wordsScore.put(normalCategory, maxScore);
                normalCategory = Constants.NOISE;
            }
        }
    }

    private Boolean ruleClassifyLayerOne() {
        for (Map.Entry<String, Set> entry : Rule.RULE_FOR_LAYER_ONE.entrySet()) {
            Set<String> set = entry.getValue();
            for (String url : set) {
                if (reqURL.contains(url)) {
                    normalCategory = entry.getKey();
                    String id = Category.superCategoryId.get(normalCategory);
                    normalCategory = normalCategory + Constants.CATEGORY_ID_SPLIT + id;
                    return true;
                }
            }
        }
        return false;
    }

    private Boolean ruleClassifyLayerTwo() {
        for (Map.Entry<String, Set> entry : Rule.RULE_FOR_LAYER_TWO.entrySet()) {
            Set<String> set = entry.getValue();
            for (String url : set) {
                if (reqURL.contains(url)) {
                    normalCategory = entry.getKey();
                    return true;
                }
            }
        }
        return false;
    }

    public void commonClassify() throws IOException, JSONException, InterruptedException {
        if (this.type != Type.sensitive) {
            Map<String, Integer> keyWordsType = new HashMap<>();
            JSONObject categoryJsonObject = Category.category;
            int layer = 0;
            if (ruleClassifyLayerTwo())
                return;
            if (ruleClassifyLayerOne()) {
                categoryJsonObject = categoryJsonObject.getJSONObject(normalCategory);
                layer = 1;
            }
            for (int i = layer; i < Constants.LAYER; ) {
                Map<String, Double> wordsScore = new HashMap<>();
                calculateAndGenerateKeyWordCategory(i, categoryJsonObject, keyWordsType, wordsScore);
                calculateAndGenerateNormalCategory(keyWordsType, wordsScore);
                if (++i < Constants.LAYER && !normalCategory.equals(Constants.NOISE)) {
                    categoryJsonObject = categoryJsonObject.getJSONObject(normalCategory);
                }
//                } else if (i < Constants.LAYER && normalCategory.equals(Constants.NOISE)) {
//                    categoryJsonObject = categoryJsonObject.getJSONObject(learnCategoryName);
//                }
            }
        }
    }

    private double calculateMaxScore(int i, double score, String tempCategory, double maxScore) {
        if (i == 0) {
            if (score >= maxScore && score > Constants.SIMILAR_SCORE_LINE) {
                normalCategory = tempCategory;
                return score;
            }
            return maxScore;
        }
        if (i == 1) {
            if (score >= maxScore && score > Constants.SIMILAR_SCORE_LINE_TWO) {
                normalCategory = tempCategory;
                return score;
            }
            return maxScore;
        }
        return maxScore;
    }

    private double similarScore(String word, String type) throws IOException, InterruptedException {
        double score;
        URL url = new URL(String.format(calculate_word_distance_url, word, type));
        URLConnection urlcon = url.openConnection();
        InputStream is = urlcon.getInputStream();
        BufferedReader buffer = new BufferedReader(new InputStreamReader(is));
        String s = buffer.readLine();
        score = Double.valueOf(s);
        buffer.close();
        return score;
    }

    public boolean isRightTime() {
        return isRightTime;
    }

    public int returnType = 0;

    public String toString() {
        if (normalCategory.equals(Constants.NOISE) && sensitiveId.size() >= Constants.SENSITIVE_WORD_NUM) {
            returnType = Constants.RETURN_TYPE_SENSITIVE;
            return uid + Constants.SEPARATOR + reqTime + Constants.SEPARATOR + reqURL + Constants.SEPARATOR + titleKeyWords.getTitle();
        } else if (!normalCategory.equals(Constants.NOISE)) {
            returnType = Constants.RETURN_TYPE_NORMAL;
            return 1 + Constants.SEPARATOR + 2 + Constants.SEPARATOR + uid + Constants.SEPARATOR + Category.categorySuperId.get(normalCategory) + Constants.SEPARATOR + Category.categoryCorrespondId.get(normalCategory) + Constants.SEPARATOR + reqTime + Constants.SEPARATOR + reqHour;
        } else {
            returnType = Constants.RETURN_TYPE_NOISE;
            return reqURL;
        }
    }

    public int getReturnType() {
        return returnType;
    }

    public String getURLCategory() {
        return reqURL + Constants.SEPARATOR + normalCategory;
    }

    public String getURL() {
        return this.respPort + this.reqURL;
    }

    public static void main(String[] args) throws IOException, JSONException, InterruptedException, ParseException {
        File file = new File("/home/tian/tianhongzeng");
        BufferedReader bufferedReader = new BufferedReader(new FileReader(file));
        String s = "20160405114421\t192.168.1.104\t55368\t123.125.115.99\t中医\thttp://baike.baidu.com/view/8915.htm\tGET\tJava/1.7.0_79";
        UserDataInformation userDataInformation = new UserDataInformation(s);
        userDataInformation.commonClassify();
        userDataInformation.sensitiveClassify();
        System.out.println(userDataInformation.getURLCategory());
        System.out.println(userDataInformation.getSensitiveId());
        System.out.println(userDataInformation.htmlContent);
//        File file2 = new File("/home/tian/test");
//        BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(file2, true));
//        while((s = bufferedReader.readLine())!=null){
//            UserDataInformation userDataInformation = null;
//            try {
//                userDataInformation = new UserDataInformation(s);
//                bufferedWriter.write(userDataInformation.getURL());
//                bufferedWriter.write("\n");
//                userDataInformation.commonClassify();
//                userDataInformation.sensitiveClassify();
//                bufferedWriter.write(userDataInformation.getURLCategory());
//                bufferedWriter.write("\n");
//            } catch (ParseException e) {
//                e.printStackTrace();
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            } catch (JSONException e) {
//                e.printStackTrace();
//            }
////            System.out.print(userDataInformation.toString());
//        }
//        bufferedReader.close();
//        bufferedWriter.close();
    }
}
