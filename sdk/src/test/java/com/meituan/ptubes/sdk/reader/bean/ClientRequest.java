package com.meituan.ptubes.sdk.reader.bean;

import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import java.util.List;
import java.util.Map;

public class ClientRequest {

    private HttpMethod method;
    private String path;
    private Map<String, List<String>> pathVariables;
    private HttpHeaders headers;
    private ByteBuf body;

    public ClientRequest(
        HttpMethod method,
        String path,
        Map<String, List<String>> pathVariables,
        HttpHeaders headers,
        ByteBuf body
    ) {
        this.method = method;
        this.path = path;
        this.pathVariables = pathVariables;
        this.headers = headers;
        this.body = body;
    }

    public HttpMethod getMethod() {
        return method;
    }

    public void setMethod(HttpMethod method) {
        this.method = method;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public Map<String, List<String>> getPathVariables() {
        return pathVariables;
    }

    public void setPathVariables(Map<String, List<String>> pathVariables) {
        this.pathVariables = pathVariables;
    }

    public HttpHeaders getHeaders() {
        return headers;
    }

    public void setHeaders(HttpHeaders headers) {
        this.headers = headers;
    }

    public ByteBuf getBody() {
        return body;
    }

    public void setBody(ByteBuf body) {
        this.body = body;
    }
}
