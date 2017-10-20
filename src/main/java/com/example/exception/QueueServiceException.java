package com.example.exception;

/**
 * Created by jerry on 2017/10/22.
 */
public class QueueServiceException extends RuntimeException {

    public QueueServiceException() {
        super();
    }

    public QueueServiceException(String message) {
        super(message);
    }

    public QueueServiceException(String message, Throwable cause) {
        super(message, cause);
    }

}
