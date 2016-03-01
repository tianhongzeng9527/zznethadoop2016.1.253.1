package tools;

import java.util.HashSet;
import java.util.Set;

/**
 * Created by tian on 15-12-28.
 */
public class Constants {
    public final static int MESSAGE_LENGTH1 = 13;
    public final static int MESSAGE_LENGTH2 = 10;
    public final static int KEY_NUM = 5;
    public final static int SENSITIVE_WORD_NUM = 1;
    public final static int LAYER = 2;
    public final static String NOISE = "noise";
    public final static double SIMILAR_SCORE_LINE = 0.25;
    public final static double SIMILAR_SCORE_LINE_TWO = 0.1;
    public final static int TIME_OUT = 30000;
    public final static String SENSITIVE_CATEGORY = "sensitive";
//    public final static String SENSITIVE_URL = "http://192.168.1.106:8080/public_behavior/api/sensitive.do";
//    public final static String CATEGORY_URL = "http://192.168.1.106:8080/public_behavior/api/behavior.do";
//    public final static String WORDS_DISTANCE_URL = "http://192.168.1.106:8080/zz_nlp/wordsDistance?word1=%s&word2=%s";
    public final static String SIMILARWORDS_URL = "http://192.168.1.106:8080/zz_nlp/similarWords?word=%s&num=%s";
    public final static int RETURN_TYPE_SENSITIVE = 1;
    public final static int RETURN_TYPE_NORMAL = 2;
    public final static int RETURN_TYPE_NOISE = 3;
    public final static String SEPARATOR = ";,;";
    public final static String DECOLLATOR_FOR_HDFS_MESSAGE = "\t";
    public final static String UNABLE_CATEGORY = "unable_category";
    public final static String UNABLE_CONNECT_URL = "unable_connect url";
    public final static String PROGRAM_EXCEPTION = "program_exception";
    public final static String WRONG_FORMAT = "wrong_format";
    public final static String LEARNING = "学习";
    public final static String CATEGORY_ID_SPLIT = "_";
    public final static int NORMAL_CATEGORY_LENGTH = 4;
    public final static int SENSITIVE_CATEGORY_LENGTH = 3;
    public final static int URL_CATEGORY_LENGTH = 2;
    public final static Set<String> TITLE_SPLIT_SYMBOL = new HashSet<String>(){
        {

        }
    };
}
