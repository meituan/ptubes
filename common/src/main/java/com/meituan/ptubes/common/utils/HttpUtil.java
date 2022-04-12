package com.meituan.ptubes.common.utils;

import com.meituan.ptubes.common.log.Logger;
import com.meituan.ptubes.common.log.LoggerFactory;
import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.Charset;

public class HttpUtil {

    private static final Logger LOG = LoggerFactory.getLogger(HttpUtil.class);
    private static final int CONNECT_TIMEOUT_MILLISECONDS = 2000;
    private static final int READ_TIMEOUT_MILLISECONDS = 2000;

    private HttpUtil() {

    }

    public static String get(String url) {
        HttpURLConnection connection = null;
        String response = null;
        int code = -1;
        try {
            connection = (HttpURLConnection) new URL(url).openConnection();
            if (connection != null) {
                connection.setRequestMethod("GET");
                response = readStream(connection);
                code = connection.getResponseCode();
                if (LOG.isDebugEnabled()) {
                    LOG.debug(String.format(
                        "get %s %s %s",
                        url,
                        connection.getResponseCode(),
                        response
                    ));
                }
            } else {
                LOG.warn("can't connect to {}" + url);
            }
        } catch (Exception e) {
            LOG.warn(String.format(
                "get %s failed %s %s",
                url,
                code,
                e.getMessage()
            ));
        } finally {
            if (connection != null) {
                try {
                    connection.disconnect();
                } catch (Exception e) {
                    LOG.warn(
                        "close connection failed... " + url,
                        e
                    );
                }
            }
        }
        return response;
    }

    public static String post(
        String url,
        String content
    ) {
        HttpURLConnection connection = null;
        String response = null;
        int code = -1;
        try {
            connection = (HttpURLConnection) new URL(url).openConnection();
            if (connection != null) {
                connection.setConnectTimeout(CONNECT_TIMEOUT_MILLISECONDS);
                connection.setReadTimeout(READ_TIMEOUT_MILLISECONDS);

                connection.setRequestMethod("POST");
                writeContent(
                    connection,
                    content
                );
                response = readStream(connection);
                code = connection.getResponseCode();
                if (LOG.isDebugEnabled()) {
                    LOG.debug(String.format(
                        "post  %s %s %s",
                        url,
                        connection.getResponseCode(),
                        response
                    ));
                }
            } else {
                LOG.warn("can't connect to {}" + url);
            }
        } catch (Exception e) {
            LOG.warn(String.format(
                "post %s failed %s %s",
                url,
                code,
                e
            ));
        } finally {
            if (connection != null) {
                try {
                    connection.disconnect();
                } catch (Exception e) {
                    LOG.warn(
                        "close connection failed... " + url,
                        e
                    );
                }
            }
        }
        return response;
    }

    private static void writeContent(
        HttpURLConnection connection,
        String content
    ) {
        OutputStreamWriter out = null;
        try {
            connection.setDoOutput(true);
            out = new OutputStreamWriter(
                connection.getOutputStream(),
                Charset.forName("utf-8")
            );
            out.write(content);
        } catch (Exception e) {
            LOG.warn(String.format(
                "write content to %s failed %s",
                connection.getURL(),
                e
            ));
        } finally {
            if (out != null) {
                try {
                    out.close();
                } catch (IOException e) {
                    LOG.warn(
                        "close connection failed... " + connection.getURL(),
                        e
                    );
                }
            }
        }
    }

    private static String readStream(HttpURLConnection connection) throws IOException {

        String result = null;
        StringBuilder sb = new StringBuilder();
        InputStream is = null;
        try {
            is = new BufferedInputStream(connection.getInputStream());
            BufferedReader br = new BufferedReader(new InputStreamReader(
                is,
                Charset.forName("utf-8")
            ));
            String inputLine = "";
            while ((inputLine = br.readLine()) != null) {
                sb.append(inputLine);
            }
            result = sb.toString();
        } catch (Exception e) {
            LOG.warn(
                "read connection failed... " + connection.getURL(),
                e
            );
        } finally {
            if (is != null) {
                try {
                    is.close();
                } catch (IOException e) {
                    LOG.warn(
                        "close connection failed... " + connection.getURL(),
                        e
                    );
                }
            }
        }
        return result;
    }
}
