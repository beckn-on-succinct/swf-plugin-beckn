package com.venky.swf.plugins.beckn.tasks;

import com.venky.core.collections.IgnoreCaseMap;
import com.venky.core.util.ExceptionUtil;
import com.venky.core.util.ObjectUtil;
import com.venky.swf.db.annotations.column.ui.mimes.MimeType;
import com.venky.swf.integration.api.Call;
import com.venky.swf.integration.api.HttpMethod;
import com.venky.swf.integration.api.InputFormat;
import com.venky.swf.plugins.background.messaging.MessageAdaptor;
import com.venky.swf.plugins.background.messaging.MessageAdaptor.MessageQueue;
import com.venky.swf.plugins.background.messaging.MessageAdaptorFactory;
import com.venky.swf.plugins.beckn.messaging.CommunicationPreference;
import com.venky.swf.plugins.beckn.messaging.Mq;
import com.venky.swf.plugins.beckn.messaging.Topic;
import com.venky.swf.routing.Config;
import in.succinct.beckn.BecknException;
import in.succinct.beckn.Context;
import in.succinct.beckn.Error;
import in.succinct.beckn.Error.Type;
import in.succinct.beckn.Request;
import in.succinct.beckn.SellerException;
import in.succinct.beckn.SellerException.GenericBusinessError;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.json.simple.JSONObject;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;

public abstract class BppTask extends BecknTask {
    protected BppTask(){
        super();
    }
    public BppTask(Request request,Map<String,String> headers){
        super(request,headers);
    }

    public final Map<String,String> generateCallbackHeaders(Request callbackRequest){
        Map<String,String> headers  = new IgnoreCaseMap<>();
        if (callbackRequest.getExtendedAttributes().get("Authorization") != null){
            headers.put("Authorization",callbackRequest.getExtendedAttributes().get("Authorization"));
        }else {
            if (signatureHeaders.contains("Authorization") && getSubscriber().getPubKeyId() != null) {
                headers.put("Authorization", callbackRequest.generateAuthorizationHeader(
                        callbackRequest.getContext().getBppId(), getSubscriber().getPubKeyId()));
            }
        }
        headers.put("Content-Type", MimeType.APPLICATION_JSON.toString());
        headers.put("Accept", MimeType.APPLICATION_JSON.toString());
        return headers;
    }
    public final void execute(){
        try {
            if (getSubscriber() == null){
                throw new RuntimeException("Subscriber  not set!");
            }
            Request out = generateCallBackRequest();
            if (out != null) {
                if (ObjectUtil.isVoid(out.getContext().getAction())){
                    out.getContext().setAction("on_"+getRequest().getContext().getAction());
                }
                send(out);
            }
        }catch (Exception ex){
            sendError(ex);
        }
    }
    protected void sendError(Throwable th) {
        sendError(th,null);
    }
    protected void sendError(Throwable input, URL schemaSource) {
        Error error = new Error();

        if (input instanceof BecknException){
            BecknException exception = (BecknException) input;
            error.setMessage(exception.getMessage());
            error.setCode(exception.getErrorCode());
        }else {
            StringBuilder message = new StringBuilder();
            Throwable th = input;
            BecknException exception = null ;
            while (th != null) {
                if (th instanceof BecknException){
                    exception = (BecknException) th;
                    break;
                }
                Throwable cause = th.getCause();
                String m = th.getMessage();
                if (cause != null && m != null) {
                    int causeClassNameLength = cause.getClass().getName().length();
                    int startIndex = causeClassNameLength;
                    if (m.length() > startIndex + 2) {
                        startIndex = startIndex + 2;
                    }
                    m = m.substring(startIndex);
                }

                if (!ObjectUtil.isVoid(m)) {
                    message.append(m);
                }

                th = cause;
            }
            if (exception == null){
                exception = message.length() > 0 ? new GenericBusinessError(message.toString()) : new GenericBusinessError();
            }
            error.setMessage(exception.getMessage());
            error.setCode(exception.getErrorCode());
        }

        error.setType(Type.DOMAIN_ERROR);

        Request callBackRequest = new Request();
        callBackRequest.setContext(getRequest().getContext());
        if (!callBackRequest.getContext().getAction().startsWith("on_")){
            callBackRequest.getContext().setAction("on_"+getRequest().getContext().getAction());
        }
        callBackRequest.setError(error);
        Config.instance().getLogger(getClass().getName()).log(Level.WARNING,"Encountered Exception", input);
        send(callBackRequest,schemaSource);
    }
    protected BecknApiCall send(Request callbackRequest){
        return send(callbackRequest,null);
    }
    protected BecknApiCall send(Request callbackRequest, URL schemaSource){
        return send(null,callbackRequest,schemaSource);
    }
    protected BecknApiCall send(String overrideUrl, Request callbackRequest,URL schemaSource){
        if (callbackRequest == null){
            return null;
        }
        String url = null;
        BecknApiCall apiCall = BecknApiCall.build();
        if (!ObjectUtil.isVoid(overrideUrl)){
            apiCall.url(overrideUrl,callbackRequest.getContext().getAction());
        }else {
            apiCall.url(callbackRequest.getContext().getBapUri(),callbackRequest.getContext().getAction());
        }
        apiCall.request(callbackRequest).
                headers(generateCallbackHeaders(callbackRequest)).path("/"+callbackRequest.getContext().getAction());

        apiCall.schema(schemaSource);

        if (getSubscriber().getCommunicationPreference() == CommunicationPreference.HTTPS) {
            apiCall.call();
        }else if (getSubscriber().getCommunicationPreference() == CommunicationPreference.MQ){
            apiCall.validateRequest();
            Mq mq = getSubscriber().getMq();
            Context context = callbackRequest.getContext();
            MessageAdaptor adaptor = MessageAdaptorFactory.getInstance().getMessageAdaptor(mq.getProvider());
            Topic topic = Topic.builder(adaptor).
                    subscriber_id(context.getBapId()).country(context.getCountry()).city(context.getCity()).
                    domain(context.getDomain()).action(context.getAction()).transaction_id(context.getTransactionId()).message_id(context.getMessageId()).build();

            MessageQueue queue = mq.getMessageQueue();

            final CloudEventBuilder builder = CloudEventBuilder.v1().withId(context.getMessageId()) // this can be
                    .withType(context.getAction()) // type of event
                    .withSource(URI.create(context.getBppUri())) // event source
                    .withDataContentType("application/octet-stream")
                    .withData(callbackRequest.toString().getBytes());

            generateCallbackHeaders(callbackRequest).forEach((k,v)-> {
                String key = k.toLowerCase();
                if (key.matches("[a-z,0-9]*")) {
                    builder.withExtension(key, v);
                }
            });

            queue.publish(topic.toString(),builder.build()); //Publish to call back queue.
        }
        return apiCall;
    }

    public abstract Request generateCallBackRequest();

}
