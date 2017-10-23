package com.example;

import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.example.model.ImmutableMessageQueue;
import com.example.model.MessageQueue;
import com.example.services.AbstractQueueService;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.Optional;

public class SqsQueueService extends AbstractQueueService {


    private static final Log LOG = LogFactory.getLog(SqsQueueService.class);


    private AmazonSQSClient sqsClient;

    public SqsQueueService(AmazonSQSClient sqsClient) {
        this.sqsClient = sqsClient;
    }

    @Override
    public void push(String queueUrl, Integer delaySeconds, String messageBody) {

        sqsClient.sendMessage(new SendMessageRequest(queueUrl, messageBody).withDelaySeconds(delaySeconds));
    }

    @Override
    public MessageQueue pull(String queueUrl) {
        Optional<Message> message = sqsClient.receiveMessage(queueUrl).getMessages().stream().findFirst();

        if (!message.isPresent()) {
            LOG.info("no message found from queueUrl '" + queueUrl + "'");
        }

        return ImmutableMessageQueue.of(
                null,
                message.get().getReceiptHandle(),
                message.get().getMessageId(),
                null,
                message.get().getBody());


    }

    @Override
    public void delete(String queryUrl, String receiptHandle) {

        sqsClient.deleteMessage(queryUrl, receiptHandle);

    }


}
