package com.venky.swf.plugins.beckn.tasks;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.venky.core.collections.IgnoreCaseMap;
import com.venky.core.string.StringUtil;
import com.venky.swf.integration.api.Call;
import com.venky.swf.integration.api.HttpMethod;
import com.venky.swf.integration.api.InputFormat;
import com.venky.swf.routing.Config;
import in.succinct.beckn.Request;
import in.succinct.beckn.Response;
import org.json.simple.JSONObject;
import org.openapi4j.core.validation.ValidationException;
import org.openapi4j.operation.validator.model.impl.Body;
import org.openapi4j.operation.validator.validation.RequestValidator;
import org.openapi4j.parser.OpenApi3Parser;
import org.openapi4j.parser.model.v3.OpenApi3;

import javax.servlet.http.Cookie;
import java.io.InputStream;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;

public class BecknApiCall {
    private BecknApiCall(){

    }
    public static BecknApiCall build(){
        return new BecknApiCall();
    }
    private String url ;
    public BecknApiCall url(String url){
        this.url = url;
        return this;
    }
    public String getUrl(){
        return url;
    }
    public BecknApiCall url(String baseUrl,String relativeUrl){
        StringBuilder sUrl = new StringBuilder();
        if (baseUrl.endsWith("/")) {
            sUrl.append(baseUrl, 0, baseUrl.length()-1);
        }else {
            sUrl.append(baseUrl);
        }
        if (relativeUrl.startsWith("/")){
            sUrl.append(relativeUrl);
        }else {
            sUrl.append("/").append(relativeUrl);
        }
        url(sUrl.toString());
        return this;
    }
    private String path;
    public BecknApiCall path(String path){
        this.path = path;
        return this;
    }

    private Map<String,String> cookies = new HashMap<>();
    public BecknApiCall cookies(Cookie[] aCookies){
        for (Cookie c : aCookies){
            cookies.put(c.getName(),c.getValue());
        }
        return this;
    }

    private Map<String,String> headers = new HashMap<>();
    public BecknApiCall headers(Map<String,String> headers){
        this.headers = headers;
        return this;
    }
    public Map<String,String> getHeaders(){
        return headers;
    }
    private  Request request ;
    public BecknApiCall request(Request request){
        this.request = request;
        return this;
    }

    private  Response response;
    public Response getResponse(){
        return response;
    }
    private BecknApiCall response(Response response){
        this.response = response;
        return this;
    }
    private int status;
    private BecknApiCall status(int status){
        this.status = status;
        return this;
    }

    private Map<String,List<String>> responseHeaders;
    public BecknApiCall responseHeaders(Map<String, List<String>> responseHeaders){
        this.responseHeaders = new IgnoreCaseMap<>();
        this.responseHeaders.putAll(responseHeaders);
        return this;
    }

    private  org.openapi4j.operation.validator.model.Response openApi3Response = null;
    private org.openapi4j.operation.validator.model.Response getOpenApi3Response(){
        if (openApi3Response == null){
            openApi3Response = new org.openapi4j.operation.validator.model.Response() {
                @Override
                public int getStatus() {
                    return status;
                }

                @Override
                public Body getBody() {
                    return Body.from(response.toString());
                }

                Map<String,Collection<String>> openApiResponseHeaders = null;
                @Override
                public Map<String, Collection<String>> getHeaders() {
                    if (openApiResponseHeaders == null){
                        openApiResponseHeaders = new IgnoreCaseMap<>();
                    }
                    for (String h : responseHeaders.keySet()){
                        openApiResponseHeaders.put(h,responseHeaders.get(h));
                    }
                    return openApiResponseHeaders;
                }

                @Override
                public Collection<String> getHeaderValues(String s) {
                    return getHeaders().get(s);
                }
            };
        }
        return openApi3Response;
    }
    private org.openapi4j.operation.validator.model.Request openApi3Request = null;
    private org.openapi4j.operation.validator.model.Request getOpenApi3Request(){
        if (openApi3Request == null){
            openApi3Request = new org.openapi4j.operation.validator.model.Request() {
                @Override
                public String getURL() {
                    return url;
                }

                @Override
                public String getPath() {
                    return path;
                }

                @Override
                public Method getMethod() {
                    return Method.POST;
                }

                @Override
                public Body getBody() {
                    return Body.from(request.toString());
                }

                @Override
                public String getQuery() {
                    return "";
                }

                @Override
                public Map<String, String> getCookies() {
                    return cookies;
                }

                Map<String,Collection<String>> openApi3Headers = null;
                @Override
                public Map<String, Collection<String>> getHeaders() {
                    if (openApi3Headers == null){
                        openApi3Headers = new IgnoreCaseMap<>();
                        for (String h : headers.keySet()){
                            this.openApi3Headers.put(h, Collections.singleton(headers.get(h)));
                        }
                    }
                    return openApi3Headers;
                }

                @Override
                public Collection<String> getHeaderValues(String s) {
                    return getHeaders().get(s);
                }
            };

        }
        return openApi3Request;
    }
    private static ObjectMapper mapper = new ObjectMapper();
    private static RequestValidator requestValidator ;
    static {
        try {
            OpenApi3 api = new OpenApi3Parser().parse(Config.class.getResource("/config/core.yaml"), false);
            requestValidator = new RequestValidator(api);
        }catch (Exception eX){
            Config.instance().getLogger(BecknApiCall.class.getName()).log(Level.WARNING,"Unable to load Schema",eX);
            requestValidator = null;
        }
    }

    public BecknApiCall call(){
        validateRequest();
        Call<JSONObject> call = new Call<JSONObject>().url(url).input(request.getInner()).inputFormat(InputFormat.JSON).headers(headers).method(HttpMethod.POST);
        InputStream is = call.getResponseStream();
        if (call.hasErrors()) {
            is = call.getErrorStream();
        }
        status(call.getStatus());
        responseHeaders(call.getResponseHeaders());
        try {
            Response ackResponse = new Response(StringUtil.read(is));
            response(ackResponse);
            validateResponse();
        }catch (NullPointerException ex){
            response(null);
        }
        return this;
    }


    public void validateRequest(){
        //
        try {
            JsonNode jsonRequest = mapper.readTree(request.toString());
            requestValidator.validate(getOpenApi3Request());
        }catch (ValidationException ex){
            throw new RuntimeException(ex.toString());
        }catch (JsonProcessingException e) {
            throw new RuntimeException(e.getMessage());
        }

    }
    public  void validateResponse(){
        try {
            JsonNode jsonResponse = mapper.readTree(response.toString());
            requestValidator.validate(getOpenApi3Response(),getOpenApi3Request());
        }catch (ValidationException ex){
            throw new RuntimeException(ex.toString());
        }catch (JsonProcessingException e) {
            throw new RuntimeException(e.getMessage());
        }
    }


}
