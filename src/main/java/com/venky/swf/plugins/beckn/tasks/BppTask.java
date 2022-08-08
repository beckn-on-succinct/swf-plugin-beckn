package com.venky.swf.plugins.beckn.tasks;

import com.venky.core.collections.IgnoreCaseMap;
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
import in.succinct.beckn.Context;
import in.succinct.beckn.Error;
import in.succinct.beckn.Error.Type;
import in.succinct.beckn.Request;
import io.cloudevents.core.builder.CloudEventBuilder;
import org.json.simple.JSONObject;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;

public abstract class BppTask extends BecknTask {
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
    protected void sendError(Throwable th, String schemaSource) {
        Error error = new Error();

        StringWriter message = new StringWriter();
        th.printStackTrace(new PrintWriter(message));
        error.setMessage(message.toString());

        error.setCode("CALL-FAILED");
        error.setType(Type.DOMAIN_ERROR);

        Request callBackRequest = new Request();
        callBackRequest.setContext(getRequest().getContext());
        if (!callBackRequest.getContext().getAction().startsWith("on_")){
            callBackRequest.getContext().setAction("on_"+getRequest().getContext().getAction());
        }
        callBackRequest.setError(error);
        Config.instance().getLogger(getClass().getName()).log(Level.WARNING,"Encountered Exception", th);
        send(callBackRequest,schemaSource);
    }
    protected BecknApiCall send(Request callbackRequest){
        return send(callbackRequest,null);
    }
    protected BecknApiCall send(Request callbackRequest,String schemaSource){
        return send(null,callbackRequest,schemaSource);
    }
    protected BecknApiCall send(String overrideUrl, Request callbackRequest,String schemaSource){
        if (callbackRequest == null){
            return null;
        }

        BecknApiCall apiCall = BecknApiCall.build().url(ObjectUtil.isVoid(overrideUrl) ?
                            callbackRequest.getContext().getBapUri()+"/"+callbackRequest.getContext().getAction() : overrideUrl).request(callbackRequest).
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
