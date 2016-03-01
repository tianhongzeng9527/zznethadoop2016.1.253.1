package usrhandle;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by tian on 15-12-17.
 */
public enum Type {
    normal,
    sensitive,
    political,
    supersition,
    sexy,
    illegal,
    drug,
    other;
    public final static String POLITICAL = "political";
    public final static String SUPERSITION = "supersition";
    public final static String SEXY = "sexy";
    public final static String ILLEGAL = "illegal";
    public final static String DRUG = "drug";
    public final static String OTHER = "other";
    public final static Map<String, Type> sensitiveTypeSet = new HashMap<String, Type>() {
        {
            put(POLITICAL, political);
            put(SUPERSITION, supersition);
            put(SEXY, sexy);
            put(ILLEGAL, illegal);
            put(DRUG, drug);
            put(OTHER, other);
        }
    };
    public final static Map<String, Type> normalTypeSet = new HashMap<String, Type>() {
        {

        }
    };
    public final static List<String> normalTypeList = new ArrayList<String>() {
        {

        }
    };
    public final static List<String> sensitiveTypeList = new ArrayList<String>() {
        {
            add(POLITICAL);
            add(SUPERSITION);
            add(SEXY);
            add(ILLEGAL);
            add(DRUG);
            add(OTHER);
        }
    };
}
