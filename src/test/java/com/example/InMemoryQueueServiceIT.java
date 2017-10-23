package com.example;

import com.example.common.AbstractQueueServiceIT;
import org.junit.Test;

/**
 * Created by jerry on 2017/10/22.
 */
public class InMemoryQueueServiceIT extends AbstractQueueServiceIT{


//    @Test
    public void shouldRunInConcurrencyModeWithMultipleProducersConsumers() throws Exception {


        Integer visibilityTimeoutInSecs = 2;

        boolean runVisibilityCollector = true;
        String queueUrl = "http://sqs.us-east-2.amazonaws.com/123456789012/MyQueue";

        InMemoryQueueService inMemoryQueueService =new InMemoryQueueService(visibilityTimeoutInSecs,runVisibilityCollector);

        runQueueServiceWithMultipleProducersConsumers(queueUrl,inMemoryQueueService);



    }


}
