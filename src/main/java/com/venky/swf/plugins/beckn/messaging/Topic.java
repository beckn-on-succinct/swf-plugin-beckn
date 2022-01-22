package com.venky.swf.plugins.beckn.messaging;

import com.venky.core.math.AnyNumber;
import com.venky.core.util.Bucket;
import com.venky.core.util.ObjectUtil;
import com.venky.swf.db.annotations.column.validations.IntegerRange;
import com.venky.swf.plugins.background.messaging.MessageAdaptor;

import java.util.HashMap;
import java.util.Map;

public class Topic {
    String topic;
    private Topic(String topic){
        this.topic = topic;
    }
    public String value(){
        return topic;
    }
    public String toString(){
        return topic;
    }
    public static class TopicBuilder {
        MessageAdaptor adaptor;
        private TopicBuilder(MessageAdaptor adaptor){
            this.adaptor = adaptor;
        }
        public Topic build(){
            Bucket bucket = new Bucket();
            Map<Integer,String> levels = new HashMap<Integer,String>(){{
                put(bucket.intValue(),"ROOT");

                bucket.increment();
                put(bucket.intValue(),country);

                bucket.increment();
                put(bucket.intValue(),city);

                bucket.increment();
                put(bucket.intValue(),domain);

                bucket.increment();
                put(bucket.intValue(),action);

                bucket.increment();
                put(bucket.intValue(), ObjectUtil.equals(action,"search") ? "all" : subscriber_id);

                bucket.increment();
                put(bucket.intValue(),transaction_id);

                bucket.increment();
                put(bucket.intValue(),message_id);
            }};
            int lastIndex = bucket.intValue();
            while (lastIndex > 0 && levels.get(lastIndex) == null){
                lastIndex -- ;
            }

            StringBuilder topic = new StringBuilder();
            for (int i = 0 ; i <= lastIndex ; i ++ ) {
                String dir = levels.get(i);
                if (topic.length() > 0){
                    topic.append(adaptor.getSeparatorToken());
                }
                if (dir != null ){
                    dir = dir.replace(adaptor.getSeparatorToken(),  "_separator_" );
                    dir = dir.replace(adaptor.getSingleLevelWildCard(),"_wild_");
                    dir = dir.replace(adaptor.getMultiLevelWildCard(),"_multi_wild_");
                }
                topic.append(dir == null ? adaptor.getSingleLevelWildCard() : dir);
            }
            if (lastIndex < bucket.intValue()) {
                topic.append(adaptor.getSeparatorToken());
                topic.append(adaptor.getMultiLevelWildCard  ());
            }
            return new Topic(topic.toString());
        }
        String country = null;
        String city = null;
        String domain = null;
        String action = null;
        String subscriber_id = null;
        String transaction_id = null;
        String message_id = null;
        // /ROOT/IND/std:080/[action]/[subscriber_id]/[transaction_id]/[message_id]
        public TopicBuilder country(String country){
            this.country = country;
            return this;
        }
        public TopicBuilder city(String city){
            this.city = city;
            return this;
        }
        public TopicBuilder domain(String domain){
            this.domain = domain;
            return this;
        }
        public TopicBuilder action(String action){
            this.action = action;
            return this;
        }
        public TopicBuilder subscriber_id(String subscriber_id){
            this.subscriber_id = subscriber_id;
            return this;
        }
        public TopicBuilder transaction_id(String transaction_id){
            this.transaction_id = transaction_id;
            return this;
        }
        public TopicBuilder message_id(String message_id){
            this.message_id = message_id;
            return this;
        }
    }
    public static TopicBuilder builder(MessageAdaptor adaptor){
        return new TopicBuilder(adaptor);
    }
}
