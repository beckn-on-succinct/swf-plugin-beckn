package com.venky.swf.plugins.beckn.messaging;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class BppSubscriber extends QueueSubscriber {

    public BppSubscriber(Subscriber subscriber) {
        super(subscriber);
    }

    public Set<String> getSupportedActions() {
        return new HashSet<>(Arrays.asList("search","select","init","confirm","track","cancel","update","status"));
    }

}
