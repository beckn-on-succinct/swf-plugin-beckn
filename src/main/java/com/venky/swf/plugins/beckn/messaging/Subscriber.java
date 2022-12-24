package com.venky.swf.plugins.beckn.messaging;

import com.venky.core.util.ObjectUtil;
import com.venky.swf.plugins.beckn.tasks.BecknTask;

import java.util.Set;

public abstract class Subscriber extends in.succinct.beckn.Subscriber {
    public CommunicationPreference getCommunicationPreference(){
        return ( getMq() == null || ObjectUtil.isVoid( getMq().getProvider()) ) ? CommunicationPreference.HTTPS : CommunicationPreference.MQ;
    }
    public Mq getMq() {
        return null;
    }
    public abstract <T extends BecknTask>  Class<T> getTaskClass(String action) ;
}

