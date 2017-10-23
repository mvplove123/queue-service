package com.example;

import com.example.model.ImmutableMessageQueue;
import com.example.model.MessageQueue;
import com.example.services.AbstractQueueService;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;
import static org.apache.commons.lang3.ObjectUtils.defaultIfNull;

public class InMemoryQueueService extends AbstractQueueService {

    /**
     * Concurrent map structure used to store messages per queue name into another concurrent FIFO message queue.
     */
    private ConcurrentHashMap<String, ConcurrentLinkedQueue<MessageQueue>> messageQueues = new ConcurrentHashMap<>();


    protected InMemoryQueueService() {
    }

    protected InMemoryQueueService(Integer visibilityTimeoutInSecs,boolean runVisibilityMonitor){

        this.visibilityTimeoutInSecs = defaultIfNull(visibilityTimeoutInSecs,MIN_VISIBILITY_TIMEOUT_SECS);
        // start message checker
        startVisibilityMonitor(runVisibilityMonitor);

    }


    private void startVisibilityMonitor(boolean runVisibilityMonitor) {

        if (runVisibilityMonitor) {
            // run visibility message checker
            Thread visibilityMonitor = new Thread(new VisibilityMessageMonitor(), "inMemoryQueueService-visibilityMonitor");
            visibilityMonitor.setDaemon(true);
            visibilityMonitor.start();
        }

    }

    /**
     * Pushes a message at the end of a queue given arguments.
     * <p>This method accepts a {@code queueUrl} parameter which must be a valid url slash-separated ('/') and ending
     * with the queueName, e.g: "http://sqs.us-east-2.amazonaws.com/123456789012/MyQueue".
     * If {@code delaySeconds} is provided, this method will set the visibility of the message pushed as per below:
     * <pre>{@code
     * visibility = currentTimeMillis + delayInMillis
     * }</pre>
     *
     * @param queueUrl
     * @param delaySeconds
     * @param messageBody
     */
    @Override
    public void push(String queueUrl, Integer delaySeconds, String messageBody) {

        String queue = fromUrl(queueUrl);

        ConcurrentLinkedQueue<MessageQueue> messagesQueue = getMessagesFromQueue(queue);
        if (messagesQueue == null) {
            messagesQueue = putMessagesToQueue(queue, new ConcurrentLinkedQueue());
        }
        messagesQueue.offer(
                MessageQueue.create(delaySeconds != null ? DateTime.now().getMillis() + TimeUnit.SECONDS.toMillis(delaySeconds) : 0L,
                        messageBody));
    }

    /**
     * Pulls a message from the top of the queue given {@code queueUrl} argument. The message retrieved must be visible
     * according to its visibility timestamp (i.e equals to 0L). Any message with visibility > 0L value will be
     * skipped and considered invisible.
     *
     * <p>Note that any other messages with visibility > 0L will be checked by the {@link VisibilityMessageMonitor} and
     * reset to 0L if their invisibility period has expired.
     *
     * @param queueUrl  Queue url holding the queue name to extract
     * @return  MessageQueue instance made up with message body and receiptHandle identifier used to delete the message
     * @throws  IllegalArgumentException If queue name cannot be extracted from queueUrl argument
     */
    @Override
    public MessageQueue pull(String queueUrl) {

        String queue = fromUrl(queueUrl);

        ConcurrentLinkedQueue<MessageQueue> messagesQueue = getMessagesFromQueue(queue);

        for (MessageQueue messageQueue : messagesQueue) {

            if (messageQueue.compareAndSetVisibility(0L, DateTime.now().getMillis() + TimeUnit.SECONDS.toMillis
                    (visibilityTimeoutInSecs))) {
                return ImmutableMessageQueue.of(messageQueue);
            }
        }
        return null;
    }

    /**
     * Deletes a message from the queue given {@code queueUrl} and {@code receiptHandle} arguments.
     *
     * @param queueUrl  Queue url holding the queue name to extract
     * @param receiptHandle  Receipt handle identifier
     * @throws IllegalArgumentException If queue url is invalid
     * @throws NullPointerException If receipt handle is null
     */
    @Override
    public void delete(String queueUrl, String receiptHandle) {

        requireNonNull(receiptHandle, "receipt handle must not be null");

        String queue = fromUrl(queueUrl);

        ConcurrentLinkedQueue<MessageQueue> messagesQueue = getMessagesFromQueue(queue);

        if (messagesQueue.isEmpty()) {
            removeEmptyQueue(queue);
        }

        List<MessageQueue> recordsToDelete = messagesQueue.stream().filter(record -> StringUtils.equals(record
                .getReceiptHandle(), receiptHandle)).collect(Collectors.toList());

        messagesQueue.removeAll(recordsToDelete);

    }


    public ConcurrentLinkedQueue<MessageQueue> getMessagesFromQueue(String queue) {
        return messageQueues.get(queue);
    }


    public ConcurrentLinkedQueue<MessageQueue> putMessagesToQueue(String queue, ConcurrentLinkedQueue<MessageQueue> messagesQueue) {
        messageQueues.put(queue, messagesQueue);
        return messagesQueue;
    }

    public ConcurrentLinkedQueue<MessageQueue> removeEmptyQueue(String queue) {

        return messageQueues.remove(queue);

    }

    protected ConcurrentLinkedQueue<MessageQueue> putMessagesIntoQueue(String queue,ConcurrentLinkedQueue<MessageQueue> messagesQueue) {
        messageQueues.put(queue, messagesQueue);
        return messagesQueue;
    }
    protected class VisibilityMessageMonitor extends AbstractVisibilityMonitor {
        protected VisibilityMessageMonitor() {
        }
        /**
         * Checks and resets the visibility of messages exceeding the timeout. The update done in this method is still safe
         * as when message.getVisibility() > 0L, the message is considered invisible by the system (consumers/producers threads
         * won't see this message), that is, only the {@link VisibilityMessageMonitor} thread will access it and possibly
         * modify it. Thus, the check and reset can safely happen in a non-atomic manner.
         */
        protected void checkMessageVisibility() {
            requireNonNull(getMessageQueue(), "messageQueue variable must not be null");

            getMessageQueue().keySet().stream().forEach(queue -> getMessageQueue().get(queue).stream().filter(
                    messageQueue -> messageQueue.getVisibility() > 0L && DateTime.now().getMillis() > messageQueue
                            .getVisibility()).forEach(messageQueue -> messageQueue.setVisibility(0L)));
        }

        protected ConcurrentHashMap<String, ConcurrentLinkedQueue<MessageQueue>> getMessageQueue() {
            return messageQueues;
        }
    }
}
