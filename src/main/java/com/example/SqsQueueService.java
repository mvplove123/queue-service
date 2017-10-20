package com.example;

import com.amazonaws.services.sqs.AmazonSQSClient;
import com.example.model.MessageQueue;
import com.example.services.AbstractQueueService;

public class SqsQueueService extends AbstractQueueService {
  @Override
  public void push(String queueUrl, Integer delaySeconds, String messageBody) {

  }

  @Override
  public MessageQueue pull(String queueUrl) {
    return null;
  }

  @Override
  public void delete(String queryUrl, String receiptHandle) {

  }
  //
  // Task 4: Optionally implement parts of me.
  //
  // This file is a placeholder for an AWS-backed implementation of QueueService.  It is included
  // primarily so you can quickly assess your choices for method signatures in QueueService in
  // terms of how well they map to the implementation intended for a production environment.
  //

}
