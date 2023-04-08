package com.ospn.utils.http;

import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.ProtocolException;
import java.net.URL;

@Slf4j
public class Http {

    private static String USER_AGENT = "Mozilla/5.0";
    private static String CONTENT_TYPE = "application/json";
    private static String ACCEPT = "application/json";

    private static HttpURLConnection HttpCreate(URL obj) throws IOException, ProtocolException {
        HttpURLConnection con = (HttpURLConnection) obj.openConnection();

        con.setRequestMethod("POST");
        con.setRequestProperty("User-Agent", USER_AGENT);
        con.setRequestProperty("Content-Type", CONTENT_TYPE);
        con.setRequestProperty("Accept", ACCEPT);
        con.setDoInput(true);
        con.setDoOutput(true);
        return con;
    }

    private static String ResponseObj(HttpURLConnection con, JSONObject obj2) throws UnsupportedEncodingException, IOException {
        BufferedWriter wr = new BufferedWriter(new OutputStreamWriter(con.getOutputStream(), "UTF-8"));
        wr.write(obj2.toString());
        wr.flush();
        wr.close();

        String ret = "";
        int status = con.getResponseCode();
        if (status == HttpURLConnection.HTTP_OK) {
            BufferedReader reader = new BufferedReader(new InputStreamReader(con.getInputStream()));
            String line = null;
            while ((line = reader.readLine()) != null) {
                System.out.println(line);
                ret = ret + line;
            }
            reader.close();
            con.disconnect();
            return ret;
        } else {
            throw new IOException("response code: " + status + "," + con.getResponseMessage());
        }

    }


    public static boolean post(String url, JSONObject json) {
        try {
            URL obj = new URL(url);
            HttpURLConnection con = HttpCreate(obj);
            String retData = ResponseObj(con, json);

            JSONObject retJson = JSONObject.parseObject(retData);
            String errCode = retJson.getString("errCode");
            log.info("return data : " + retData);
            if (errCode.equalsIgnoreCase("0:success")) {
                return true;
            }


        } catch (Exception e) {
            log.info(e.getMessage());
        }
        return false;

    }
}
