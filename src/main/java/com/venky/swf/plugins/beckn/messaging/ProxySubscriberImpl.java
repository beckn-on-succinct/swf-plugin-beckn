package com.venky.swf.plugins.beckn.messaging;

import com.venky.swf.plugins.beckn.tasks.BecknTask;

import java.util.Set;

public class ProxySubscriberImpl extends Subscriber {
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
    public CommunicationPreference getCommunicationPreference() {
        return subscriber.getCommunicationPreference();
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

    protected final Subscriber subscriber;
    public ProxySubscriberImpl(com.venky.swf.plugins.beckn.messaging.Subscriber subscriber){
        this.subscriber = subscriber;
    }


}
