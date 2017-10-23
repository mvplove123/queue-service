package com.example;

import com.example.model.MessageQueue;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.Iterables;

import static com.example.services.StringUtils.requireNonEmpty;
import static com.google.common.base.Preconditions.checkArgument;

public interface QueueService {

    /**
     * Pushes a message at the end of a queue given {@code queueUrl}, {@code delaySeconds} and {@code messageBody} arguments.
     * @param queueUrl
     * @param delaySeconds
     * @param messageBody
     */
    void push(String queueUrl, Integer delaySeconds, String messageBody);

    /**
     * Pulls a message from the top of the queue given {@code queueUrl} argument.
     * @param queueUrl
     * @return
     */
    MessageQueue pull(String queueUrl);


    /**
     * Deletes a message from the queue given {@code queueUrl} and {@code receiptHandle} arguments.
     * @param queryUrl
     * @param receiptHandle
     */
    void delete(String queryUrl, String receiptHandle);

    /**
     * Extracts queue name from a given {@code queueUrl} argument value.
     * @param queueUrl
     * @return
     */
    default String fromUrl(String queueUrl) {
        checkArgument(!Strings.isNullOrEmpty(queueUrl), "queueUrl must not be empty");

        return requireNonEmpty(Iterables.getLast(Splitter.on("/").split(queueUrl), null),
                "queueName must not be empty");


    }


}
