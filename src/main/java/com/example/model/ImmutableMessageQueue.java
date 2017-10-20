package com.example.model;

import com.google.common.base.Objects;

import java.io.Serializable;

import static java.util.Objects.requireNonNull;

/**
 * Created by admin on 2017/10/17.
 */
public final class ImmutableMessageQueue extends MessageQueue implements Serializable, Cloneable {

    private Integer requeueCount;

    private String receiptHandle;

    private String messageId;

    private Long visibility;

    private String messageBody;


    //消息对象构造函数
    public ImmutableMessageQueue(Integer requeueCount, String receiptHandle,
                                 String messageId, Long visibilityDelay, String messageBody){
        this.requeueCount = requireNonNull(requeueCount);
        this.receiptHandle = requireNonNull(receiptHandle);
        this.messageId = requireNonNull(messageId);
        this.visibility = requireNonNull(visibilityDelay);
        this.messageBody = requireNonNull(messageBody);


    }

    //返回一个不可变消息对象
    public static ImmutableMessageQueue of (MessageQueue messageQueue){
            return  new ImmutableMessageQueue(
                    messageQueue.getRequestCount(),
                    messageQueue.getReceiptHandle(),
                    messageQueue.getMessageId(),
                    messageQueue.getVisibility(),
                    messageQueue.getMessageBody());
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        ImmutableMessageQueue that = (ImmutableMessageQueue) o;
        return Objects.equal(messageId, that.messageId);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(super.hashCode(), messageId);
    }

    public Integer getRequeueCount() {
        return requeueCount;
    }


    public void setVisibility(long visibility) {
        throw new UnsupportedOperationException();
    }


    @Override
    public String getReceiptHandle() {
        return receiptHandle;
    }

    @Override
    public String getMessageId() {
        return messageId;
    }

    @Override
    public Long getVisibility() {
        return visibility;
    }

    @Override
    public String getMessageBody() {
        return messageBody;
    }
}