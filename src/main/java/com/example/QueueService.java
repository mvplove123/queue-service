package com.example;

import com.amazonaws.util.StringUtils;
import com.example.model.MessageQueue;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.Iterables;

import static com.example.services.StringUtils.requireNonEmpty;
import static com.google.common.base.Preconditions.checkArgument;

public interface QueueService {


    void push(String queueUrl, Integer delaySeconds, String messageBody);

    MessageQueue pull(String queueUrl);


    void delete(String queryUrl, String receiptHandle);


    default String fromUrl(String queueUrl) {
        checkArgument(!Strings.isNullOrEmpty(queueUrl), "queueUrl must not be empty");

        return requireNonEmpty(Iterables.getLast(Splitter.on("/").split(queueUrl), null),
                "queueName must not be empty");


    }


}
