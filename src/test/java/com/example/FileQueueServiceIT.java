package com.example;

import com.example.common.AbstractQueueServiceIT;
import org.junit.Test;

import java.util.concurrent.ExecutionException;

/**
 * Created by jerry on 2017/10/22.
 * Integration test verifying the correct execution of the {@link FileQueueService} class while being invoked by
 * multiple concurrent producers consumers.
 */

public class FileQueueServiceIT extends AbstractQueueServiceIT {
//    @Test
    public void shouldRunInConcurrencyModeWithMultipleProducersConsumers() throws ExecutionException, InterruptedException {
        String baseDirPath = null;
        Integer visibilityTimeoutInSecs = 2;
        boolean addShutdownHook = true;
        String queueUrl = "http://sqs.us-east-2.amazonaws.com/123456789012/MyQueue";

        FileQueueService queueService = new FileQueueService(baseDirPath, visibilityTimeoutInSecs, addShutdownHook);

        runQueueServiceWithMultipleProducersConsumers(queueUrl, queueService);
    }

}
