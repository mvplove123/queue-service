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

    private ConcurrentHashMap<String, ConcurrentLinkedQueue<MessageQueue>> messageQueues = new ConcurrentHashMap<>();


    protected InMemoryQueueService() {
    }

    protected InMemoryQueueService(Integer visibilityTimeoutInSecs,boolean runVisibilityMonitor){

        this.visibilityTimeoutInSecs = defaultIfNull(visibilityTimeoutInSecs,MIN_VISIBILITY_TIMEOUT_SECS);

        startVisibilityMonitor(runVisibilityMonitor);

    }


    private void startVisibilityMonitor(boolean runVisibilityMonitor) {

        if (runVisibilityMonitor) {
            // run visibility message checker

            Thread visibilityMonitor = new Thread(new VisibilityMessageMonitor());
        }

    }


    @Override
    public void push(String queueUrl, Integer delaySeconds, String messageBody) {

        String queue = fromUrl(queueUrl);

        //获取指定名称的消息队列，如果为空，则创建，然后把消息添加到消息队列里，有效时间为当前时间+延长时间
        ConcurrentLinkedQueue<MessageQueue> messagesQueue = getMessagesFromQueue(queue);
        if (messagesQueue == null) {
            messagesQueue = putMessagesToQueue(queue, new ConcurrentLinkedQueue());
        }
        messagesQueue.offer(
                MessageQueue.create(delaySeconds != null ? DateTime.now().getMillis() + TimeUnit.SECONDS.toMillis(delaySeconds) : 0L,
                        messageBody));
    }

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

    @Override
    public void delete(String queryUrl, String receiptHandle) {

        requireNonNull(receiptHandle, "receipt handle must not be null");

        String queue = fromUrl(queryUrl);

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
