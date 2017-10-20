package com.example.services;

import com.example.QueueService;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import static org.apache.commons.lang3.ObjectUtils.defaultIfNull;

/**
 * Created by admin on 2017/10/17.
 */
public abstract class AbstractQueueService implements QueueService {


    protected static final int MIN_VISIBILITY_TIMEOUT_SECS = 60;

    protected int visibilityTimeoutInSecs = MIN_VISIBILITY_TIMEOUT_SECS;

    protected boolean stopVisibilityCollector = false;


    protected abstract class AbstractVisibilityMonitor implements Runnable {

        private final Log LOG = LogFactory.getLog(AbstractVisibilityMonitor.class);

        protected static final long VISIBILITY_MONITOR_PAUSE_TIMER = 2000L;

        protected long pauseTimer = VISIBILITY_MONITOR_PAUSE_TIMER;


        public AbstractVisibilityMonitor() {
        }


        public AbstractVisibilityMonitor(long pauseTimer) {
            this.pauseTimer = defaultIfNull(pauseTimer, VISIBILITY_MONITOR_PAUSE_TIMER);
        }

        public void run() {

            LOG.info("Starting VisibilityMessageMonitor...");

            while (!stopVisibilityCollector) {
                checkMessageVisibility();

                try {
                    Thread.sleep(pauseTimer);

                } catch (InterruptedException e) {
                    LOG.error("An exception occurred while pausing the visibility collector", e);
                }
            }

        }
        protected abstract void checkMessageVisibility();


    }



}
