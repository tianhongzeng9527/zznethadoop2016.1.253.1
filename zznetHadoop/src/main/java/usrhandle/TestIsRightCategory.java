package usrhandle;

import tools.Constants;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.util.Map;

/**
 * Created by tian on 16-1-25.
 */
public class TestIsRightCategory {
    public static void main(String[] args) throws IOException, InterruptedException {
        Map<String, String> categorys = Category.categoryCorrespondId;
        for(Map.Entry<String, String> map:categorys.entrySet()){
            similarScore("娱乐",map.getKey());
        }
    }
    private static double similarScore(String word, String type) throws IOException, InterruptedException {
        double score;
        URL url = new URL(String.format("", word, type));
        URLConnection urlcon = url.openConnection();
        InputStream is = urlcon.getInputStream();
        BufferedReader buffer = new BufferedReader(new InputStreamReader(is));
        String s = buffer.readLine();
        score = Double.valueOf(s);
        buffer.close();
        System.out.println(word+"  "+type+"  "+score);
        return score;
    }
}
