package com.example;

import com.example.model.MessageQueue;

public interface QueueService {

  //
  // Task 1: Define me.
  //
  // This interface should include the following methods.  You should choose appropriate
  // signatures for these methods that prioritise simplicity of implementation for the range of
  // intended implementations (in-memory, file, and SQS).  You may include additional methods if
  // you choose.
  //
  // - push
  //   pushes a message onto a queue.
  // - pull
  //   retrieves a single message from a queue.
  // - delete
  //   deletes a message from the queue that was received by pull().
  //

    void push(String queueUrl,Integer delaySeconds,String messageBody);

    MessageQueue pull(String queueUrl);


    void delete(String queryUrl , String receiptHandle);




}
