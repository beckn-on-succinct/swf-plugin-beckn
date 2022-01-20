package com.venky.swf.plugins.beckn.tasks;

import in.succinct.beckn.Request;

import java.util.Map;

public abstract class BapTask extends BecknTask {

    public BapTask(Request request, Map<String, String> headers) {
        super(request, headers);
    }


}
