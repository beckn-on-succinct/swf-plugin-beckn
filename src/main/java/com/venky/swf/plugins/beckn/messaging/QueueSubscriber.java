package com.venky.swf.plugins.beckn.messaging;

import com.venky.core.collections.IgnoreCaseMap;
import com.venky.core.io.ByteArrayInputStream;
import com.venky.core.string.StringUtil;
import com.venky.swf.plugins.background.core.AsyncTaskManager;
import com.venky.swf.plugins.background.core.TaskManager;
import com.venky.swf.plugins.background.messaging.MessageAdaptor;
import com.venky.swf.plugins.background.messaging.MessageAdaptor.CloudEventHandler;
import com.venky.swf.plugins.background.messaging.MessageAdaptor.MessageQueue;
import com.venky.swf.plugins.background.messaging.MessageAdaptor.SubscriptionHandle;
import com.venky.swf.plugins.background.messaging.MessageAdaptorFactory;
import com.venky.swf.plugins.beckn.tasks.BecknTask;
import com.venky.swf.routing.Config;
import in.succinct.beckn.Request;
import io.cloudevents.CloudEvent;
import io.cloudevents.CloudEventData;

import java.util.Collections;
import java.util.Map;
import java.util.logging.Level;

public class QueueSubscriber extends ProxySubscriberImpl {

    public QueueSubscriber(Subscriber subscriber){
        super(subscriber);
    }



    public void registerSubscriber(){
        QueueSubscriber self = this;
        Mq mq = getMq();
        MessageAdaptor adaptor = MessageAdaptorFactory.getInstance().getMessageAdaptor(mq.getProvider());
        MessageQueue queue = mq.getMessageQueue();
        for (String action : getSupportedActions()){
            Topic topic = Topic.builder(adaptor).subscriber_id(getSubscriberId())
                    .domain(getDomain()).action(action).build();
            queue.subscribe(topic.value(), new CloudEventHandler() {
                @Override
                public void handle(String topic, CloudEvent event, SubscriptionHandle subscriptionHandle) {
                    try {
                        Config.instance().getLogger(getClass().getName()).log(Level.INFO,String.format("Received into topic %s",topic));
                        CloudEventData data = event.getData();
                        if (data != null){
                            String payload = StringUtil.read(new ByteArrayInputStream(data.toBytes()));
                            Request request = new Request(payload);
                            //Note these changes will not tamper ther payload string and signature validations will still work.
                            request.getContext().setAction(action);
                            if (action.startsWith("on_")){
                                request.getContext().setBapId(getSubscriberId());
                                request.getContext().setBapUri(getSubscriberUrl());
                            }else {
                                request.getContext().setBppId(getSubscriberId());
                                request.getContext().setBppUri(getSubscriberUrl());
                            }

                            Map<String,String> headers = new IgnoreCaseMap<>();
                            event.getExtensionNames().forEach(n->headers.put(n,(String)event.getExtension(n)));
                            BecknTask task = getTaskClass(action).getConstructor(Request.class, Map.class).newInstance(request,headers);
                            task.setSubscriber(self);
                            if (headers.containsKey("Authorization")) {
                                task.registerSignatureHeaders("Authorization");
                            }
                            if (task.async()){
                                AsyncTaskManager.getInstance().addAll(Collections.singletonList(task));
                            }else {
                                TaskManager.instance().execute(task);
                            }
                        }
                    }catch (Exception ex){
                        Config.instance().getLogger(getClass().getName()).log(Level.WARNING,"Could handle message received  in topic " + topic,ex);
                    }
                }
            });
        }
    }


}
