package com.example.common;

import com.example.QueueService;
import com.example.model.MessageQueue;
import com.google.common.collect.Lists;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by admin on 2017/10/17.
 */
public abstract class AbstractQueueServiceIT {


    private static final Log LOG = LogFactory.getLog(AbstractQueueServiceIT.class);

    protected void runQueueWithMuitiProducersConsumers(String queueUrl, QueueService queueService) throws InterruptedException, ExecutionException {

        final AtomicInteger counter = new AtomicInteger(0);


        List<Callable<String>> pushers = Lists.newArrayList();

        ExecutorService executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());


        for (int i = 0; i < 100; i++) {

            Callable worker = () -> {
                String messageBody = "message" + counter.incrementAndGet();
                queueService.push(queueUrl, null, messageBody);
                LOG.info("producer inserted message : " + messageBody);

                return null;

            };
            pushers.add(worker);
        }
        executor.invokeAll(pushers);

        for (int i = 0; i < 59; i++) {


            Callable<String> workPuller = () -> {

                MessageQueue messageQueue = queueService.pull(queueUrl);
                LOG.info("messageQueue pulled by consumer : "+ messageQueue);
                return messageQueue.getReceiptHandle();

            };

            String receiptHandle = executor.submit(workPuller).get();


            Runnable workerRemover = () -> {
                queueService.delete(queueUrl, receiptHandle);
                LOG.info("consumer deleted message with receipt handle : " + receiptHandle);

            };

            if (i % 2 == 0) {
                executor.execute(workerRemover);
            }
        }


        executor.shutdown();

        executor.awaitTermination(10000L, TimeUnit.SECONDS);


        Thread.sleep(2000L);
        MessageQueue messageQueue = queueService.pull(queueUrl);

        LOG.info("consumer pulled message : " + messageQueue);
        Thread.sleep(2000L);
        messageQueue = queueService.pull(queueUrl);

        LOG.info("consumer pulled message : " + messageQueue);
        Thread.sleep(2000L);

        messageQueue = queueService.pull(queueUrl);

        LOG.info("consumer pulled message : " + messageQueue);

        // pause 30 secs to verify how many messages the visibility monitor thread has re-enable
        Thread.sleep(30000L);
    }

}
