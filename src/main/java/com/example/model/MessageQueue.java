package com.example.model;

import com.google.common.base.Joiner;
import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;

import java.io.Serializable;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by jerry on 2017/10/22.
 * Class representing a message queue and holding the message details, namely its requeue count,
 * receipt handle, message id, visibility and message body.
 */
public class MessageQueue implements Serializable, Cloneable {

    private Integer reqeueCount;

    private String receiptHandle;

    private String messageId;

    private String messageBody;

    private AtomicLong visibility = new AtomicLong(0L);

    protected MessageQueue() {
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MessageQueue that = (MessageQueue) o;
        return Objects.equal(messageId, that.messageId);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(messageId);
    }

    public MessageQueue(Integer requestCount, String receiptHandle, String messageId, String messageBody) {
        this.reqeueCount = requestCount;
        this.receiptHandle = receiptHandle;
        this.messageId = messageId;
        this.messageBody = messageBody;
    }

    public MessageQueue(String messageBody) {
        this.messageBody = messageBody;
        this.reqeueCount = 0;
        this.receiptHandle = UUID.randomUUID().toString();
        this.messageId = UUID.randomUUID().toString();
    }

    public Integer getRequeueCount() {
        return reqeueCount;
    }

    public void setRequeueCount(Integer reqeueCount) {
        this.reqeueCount = reqeueCount;
    }

    public String getReceiptHandle() {
        return receiptHandle;
    }

    public void setReceiptHandle(String receiptHandle) {
        this.receiptHandle = receiptHandle;
    }

    public String getMessageId() {
        return messageId;
    }

    public void setMessageId(String messageId) {
        this.messageId = messageId;
    }

    public String getMessageBody() {
        return messageBody;
    }

    public void setMessageBody(String messageBody) {
        this.messageBody = messageBody;
    }

    public Long getVisibility() {
        return visibility.get();
    }

    /**
     * Resets the visibility value.
     * @param visibility
     */
    public void setVisibility(Long visibility) {
        this.visibility.set(visibility);
    }

    /**
     * Compares and sets atomically the visibility value by invoking {@link AtomicLong#compareAndSet(long, long)} method
     * @param expect
     * @param update
     * @return
     */
    public boolean compareAndSetVisibility(long expect , long update){
            return visibility.compareAndSet(expect,update);
    }

    public static MessageQueue create(long visibileFrom, String messageBody) {
        MessageQueue messageQueue = new MessageQueue(messageBody);
        messageQueue.visibility.set(visibileFrom);
        return messageQueue;
    }


    public static MessageQueue createFromLine(String messageLine){

        List<String> recordFields = Lists.newArrayList(Splitter.on(":").split(messageLine));

        String requeueCount = recordFields.get(0);
        String visibility = recordFields.get(1);
        String receiptHandle = recordFields.get(2);
        String messageId = recordFields.get(3);
        String messageBody = recordFields.get(4);

        MessageQueue messageQueue = new MessageQueue(Ints.tryParse(requeueCount), receiptHandle, messageId, messageBody);
        messageQueue.visibility.set(Longs.tryParse(visibility));

        return messageQueue;

    }


    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("reqeueCount", reqeueCount)
                .add("receiptHandle", receiptHandle)
                .add("messageId", messageId)
                .add("messageBody", messageBody)
                .add("visibility", visibility)
                .toString();
    }



    public String writeToString(){
    return Joiner.on(":").skipNulls().join(reqeueCount,visibility.get(),receiptHandle,messageId,messageBody);
    }

}
