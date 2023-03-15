package com.venky.swf.plugins.beckn.tasks;

import com.venky.core.util.MultiException;
import com.venky.swf.plugins.background.core.Task;
import com.venky.swf.plugins.beckn.messaging.Subscriber;
import in.succinct.beckn.Request;

import java.io.Serializable;
import java.security.SignatureException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public abstract class BecknTask implements Task , Serializable {
    private Request request;
    private Map<String,String> headers;
    protected BecknTask(){

    }
    public BecknTask(Request request, Map<String,String> headers){
        this.request = request;
        this.headers = headers;
        if (this.headers == null){
            this.headers = new HashMap<>();
        }
    }

    Set<String> signatureHeaders = new HashSet<>();
    public void registerSignatureHeaders(String ... headers){
        if (headers != null && headers.length >0){
            signatureHeaders.addAll(Arrays.asList(headers));
        }
    }

    public Map<String, String> getHeaders() {
        return headers;
    }

    public Request getRequest() {
        return request;
    }

    public boolean verifySignatures(boolean throwOnError){
        MultiException ex = new MultiException();
        for (String header  : signatureHeaders){
            if (!request.verifySignature(header,headers)){
                ex.add(new SignatureException(String.format("%s:%s could not be verified!" , header, headers.get(header))));
            }
        }
        if (throwOnError && !ex.isEmpty()){
            throw  ex;
        }
        return ex.isEmpty();
    }

    public boolean async(){
        return true;
    }


    private transient Subscriber subscriber = null;
    public Subscriber getSubscriber() {
        return subscriber;
    }
    public void setSubscriber(Subscriber subscriber){
        this.subscriber = subscriber;
    }



}
