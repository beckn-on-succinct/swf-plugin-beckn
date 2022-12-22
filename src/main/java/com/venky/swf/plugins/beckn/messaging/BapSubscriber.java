package com.venky.swf.plugins.beckn.messaging;

import java.util.Set;

public class BapSubscriber extends QueueSubscriber{
    public BapSubscriber(Subscriber subscriber) {
        super(subscriber);
    }

    public Set<String> getSupportedActions() {
        return in.succinct.beckn.Subscriber.BAP_ACTION_SET;
    }
}
