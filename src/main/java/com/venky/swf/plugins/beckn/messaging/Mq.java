package com.venky.swf.plugins.beckn.messaging;

import com.venky.swf.plugins.background.messaging.MessageAdaptor.MessageQueue;
import com.venky.swf.plugins.background.messaging.MessageAdaptorFactory;

import java.util.HashMap;
import java.util.Map;

public interface Mq {
    default String getProvider(){
        return  null;
    }
    default String getHost() {
        return null;
    }
    default String getPort() {
        return null;
    }
    default String getUser() {
        return null;
    }
    default String getPassword(){
        return null;
    }
    default Map<String,String> getConnectionParams(){
        return new HashMap<String,String>(){{
            put("host",getHost());
            put("port",getPort());
            put("user",getUser());
            put("password",getPassword());
        }};
    }
    default MessageQueue getMessageQueue(){
        return MessageAdaptorFactory.getInstance().getMessageAdaptor(getProvider()).getMessageQueue(getConnectionParams());
    }

}
