package com.venky.swf.plugins.beckn.messaging;

import com.venky.core.util.ObjectUtil;
import com.venky.swf.plugins.beckn.tasks.BecknTask;

import java.util.Set;

public interface Subscriber {

    public String getSubscriberUrl();
    public String getSubscriberId();
    public String getPubKeyId();
    public String getDomain();
    public Set<String> getSupportedActions();

    default CommunicationPreference getCommunicationPreference(){
        return ( getMq() == null || ObjectUtil.isVoid( getMq().getProvider()) ) ? CommunicationPreference.HTTPS : CommunicationPreference.MQ;
    }

    public Mq getMq();

    public abstract <T extends BecknTask>  Class<T> getTaskClass(String action);
}

