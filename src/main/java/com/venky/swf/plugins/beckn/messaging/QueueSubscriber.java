package com.venky.swf.plugins.beckn.messaging;

import com.venky.core.collections.IgnoreCaseMap;
import com.venky.core.io.ByteArrayInputStream;
import com.venky.core.string.StringUtil;
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

import java.util.Map;
import java.util.Set;
import java.util.logging.Level;

public class QueueSubscriber implements Subscriber {
    @Override
    public CommunicationPreference getCommunicationPreference() {
        return subscriber.getCommunicationPreference();
    }

    @Override
    public String getSubscriberUrl() {
        return subscriber.getSubscriberUrl();
    }

    @Override
    public String getSubscriberId() {
        return subscriber.getSubscriberId();
    }

    @Override
    public String getPubKeyId() {
        return subscriber.getPubKeyId();
    }

    @Override
    public String getDomain() {
        return subscriber.getDomain();
    }

    @Override
    public Mq getMq() {
        return subscriber.getMq();
    }

    @Override
    public Set<String> getSupportedActions() {
        return subscriber.getSupportedActions();
    }

    @Override
    public <T extends BecknTask> Class<T> getTaskClass(String action) {
        return subscriber.getTaskClass(action);
    }

    Subscriber subscriber;
    public QueueSubscriber(Subscriber subscriber){
        this.subscriber = subscriber;
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
                        CloudEventData data = event.getData();
                        if (data != null){
                            String payload = StringUtil.read(new ByteArrayInputStream(data.toBytes()));
                            Request request = new Request(payload);
                            Map<String,String> headers = new IgnoreCaseMap<>();
                            event.getExtensionNames().forEach(n->headers.put(n,(String)event.getExtension(n)));
                            BecknTask task = getTaskClass(action).getConstructor(Request.class, Map.class).newInstance(request,headers);
                            task.setSubscriber(self);
                            if (task.async()){
                                TaskManager.instance().executeAsync(task,false);
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
