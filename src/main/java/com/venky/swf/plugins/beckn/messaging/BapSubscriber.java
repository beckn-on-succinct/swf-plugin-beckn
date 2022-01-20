package com.venky.swf.plugins.beckn.messaging;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class BapSubscriber extends QueueSubscriber{
    public BapSubscriber(Subscriber subscriber) {
        super(subscriber);
    }

    public Set<String> getSupportedActions() {
        return new HashSet<>(Arrays.asList("on_search","on_select","on_init","on_confirm","on_track","on_cancel","on_update","on_status"));
    }
}
