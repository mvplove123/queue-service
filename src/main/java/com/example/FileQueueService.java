package com.example;

import com.example.exception.QueueServiceException;
import com.example.model.ImmutableMessageQueue;
import com.example.model.MessageQueue;
import com.example.services.AbstractQueueService;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.primitives.Longs;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.joda.time.DateTime;

import java.io.*;
import java.nio.file.Files;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.example.commonUtils.Utils.createDirectory;
import static com.example.commonUtils.Utils.createFile;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;
import static org.apache.commons.lang3.ObjectUtils.defaultIfNull;

public class FileQueueService extends AbstractQueueService {
    private static final Log LOG = LogFactory.getLog(FileQueueService.class);


    private String canvaDirPath;


    private static final String MESSAGES_FILE_NAME = "messages";

    private static final String NEW_MESSAGES_FILE_NAME = "messages.new";

    private static final String LOCK_DIR_NAME = ".lock";

    private CopyOnWriteArrayList<File> lockFiles = Lists.newCopyOnWriteArrayList();

    private ConcurrentHashMap<String, ReentrantLock> threadLockMap = new ConcurrentHashMap<>();

    private static final String DEFAULT_CANVA_DIR = System.getProperty("user.home") + File.separator + ".canva";

    /**
     * Default constructor
     */
    protected FileQueueService() {
    }

    public FileQueueService(String baseDirPath, Integer visibilityTimeoutInSecs, boolean addShutdownHook) {
        this.canvaDirPath = !StringUtils.isEmpty(baseDirPath) ? baseDirPath : DEFAULT_CANVA_DIR;
        this.visibilityTimeoutInSecs = defaultIfNull(visibilityTimeoutInSecs, MIN_VISIBILITY_TIMEOUT_SECS);

        // create canva base directory
        createDirectory(canvaDirPath);

        // add shutdown hook to remove lock file left when application shutdown
        addFileLockShutdownHook(addShutdownHook);
    }

    /**
     * Shutdown hook class that removes the remaining lock files before exiting the program. This class allows the
     * program to cleanly shutdown and prevents any issue while restarting it later on.
     *
     * @see FileQueueService#lockFiles
     */
    private void addFileLockShutdownHook(boolean addShutdownHook) {
        if (addShutdownHook) {
            Runtime.getRuntime().addShutdownHook(new Thread(new FileLockShutdownHook(), "fileQueueService-shutdownHook"));
        }
    }


    @Override
    public void push(String queueUrl, Integer delaySeconds, String messageBody) {

        String queue = fromUrl(queueUrl);
        File fileMessages = getMessagesFile(queue);
        File lock = getLockFile(queue);
        long visibleFrom = (delaySeconds != null) ? DateTime.now().getMillis() + TimeUnit.SECONDS.toMillis(delaySeconds) : 0L;

        lock(lock);

        try (PrintWriter pw = getPrintWriter(fileMessages)) {

            // create messageQueue
            MessageQueue messageQueue = MessageQueue.create(visibleFrom, messageBody);
            // add messageQueue to file
            pw.println(messageQueue.writeToString());

        } catch (IOException e) {
            throw new QueueServiceException("An error occurred while pushing messages [" + messageBody + "] to file '" + fileMessages.getPath() + "'", e);
        } finally {
            unlock(lock);
        }


    }


    protected PrintWriter getPrintWriter(File fileMessages) throws IOException {

        return new PrintWriter(new FileWriter(fileMessages, true));
    }


    @Override
    public MessageQueue pull(String queueUrl) {
        String queue = fromUrl(queueUrl);
        File fileMessages = getMessagesFile(queue);
        File newFileMessages = getNewMessagesFile(queue);
        File lock = getLockFile(queue);
        MessageQueue messageQueue = null;

        lock(lock);

        try {

            // read list of messages from file
            try (BufferedReader reader = getBufferedReader(fileMessages);
                 PrintWriter writer = getPrintWriter(newFileMessages)) {

                String[] linesArray = Iterators.toArray(reader.lines().iterator(), String.class);

                // create a reusable stream supplier
                Supplier<Stream<String>> streamSupplier =
                        () -> Stream.of(linesArray);

                // find first visible line to pull
                Optional<String> visibleLineToPull = streamSupplier
                        .get()
                        .filter(s -> isVisibleLine(s))
                        .findFirst();

                if (!visibleLineToPull.isPresent()) {
                    LOG.error("no visible messageQueue could be found in file '" + fileMessages.getPath() + "'");
                    return null;
                }

                // change pulled message visibility
                String updatedMessageLine = updateMessageVisibility(visibleLineToPull.get(), visibilityTimeoutInSecs);

                // write new visibility message to file
                writeNewVisibilityToFile(streamSupplier, writer, updatedMessageLine);

                // create messageQueue object
                messageQueue = MessageQueue.createFromLine(updatedMessageLine);
            }

            // replace file messages with new file
            replaceWithNewFile(fileMessages, newFileMessages);

        } catch (IOException e) {
            throw new QueueServiceException("An exception occurred while pulling from queue '" + queue + "'", e);
        } finally {
            unlock(lock);
        }

        return ImmutableMessageQueue.of(messageQueue);
    }

    @Override
    public void delete(String queryUrl, String receiptHandle) {


        requireNonNull(receiptHandle, "receipt handle must not be null");

        String queue = fromUrl(queryUrl);

        File fileMessages = getMessagesFile(queue);

        File newFileMessages = getNewMessagesFile(queue);

        File lock = getLockFile(queue);

        lock(lock);

        try {

            // read from messages file and retain all but the line containing the receipt handle
            List<String> linesWithoutReceiptHandle = getLinesFromFileMessages(fileMessages)
                    .stream()
                    .filter(s -> !s.contains(receiptHandle))
                    .collect(Collectors.toList());

            // write lines to temp file
            writeLinesToNewFile(newFileMessages, linesWithoutReceiptHandle);

            // replace file messages with new file
            replaceWithNewFile(fileMessages, newFileMessages);

        } catch (IOException e) {
            throw new QueueServiceException("An exception occurred while deleting receiptHandle '" + receiptHandle + "'", e);
        } finally {
            unlock(lock);
        }


    }


    protected File getMessageFile(String queueName) {

        checkArgument(!Strings.isNullOrEmpty(queueName), "queueName is not null");

        createDirectory(canvaDirPath + File.separator + queueName);

        return createFile(new File(canvaDirPath + File.separator + queueName + File.separator + MESSAGES_FILE_NAME));

    }


    protected File getLockFile(String queueName) {

        checkArgument(!Strings.isNullOrEmpty(queueName), "queueName is not null");

        createDirectory(canvaDirPath + File.separator + queueName);

        return createFile(new File(canvaDirPath + File.separator + queueName + File.separator + LOCK_DIR_NAME));

    }


    protected void lock(File lock) {

        getThreadLock(lock).lock();

        try {
            while (!lock.mkdir()) {
                Thread.sleep(50);
            }
        } catch (InterruptedException e) {

            throw new QueueServiceException("An exception occurred while creating lock file '" + lock + ";", e);
        }

    }

    protected void unlock(File lock) {

        lock.delete();

        lockFiles.remove(lock);

        getThreadLock(lock).unlock();
    }


    private ReentrantLock getThreadLock(File lock) {

        ReentrantLock threadLock = threadLockMap.get(lock.getPath());

        if (threadLock == null) {
            threadLock = new ReentrantLock();
            threadLockMap.put(lock.getPath(), threadLock);
        }

        return threadLock;

    }


    protected File getNewMessagesFile(String queueName) {

        checkArgument(!Strings.isNullOrEmpty(queueName), "queueName must not bet null");

        createDirectory(canvaDirPath + File.separator + queueName);

        return createFile(new File(canvaDirPath + File.separator + queueName + File.separator + NEW_MESSAGES_FILE_NAME));

    }

    protected BufferedReader getBufferedReader(File fileMessages) throws IOException {

        return Files.newBufferedReader(fileMessages.toPath());

    }


    protected List<String> getLinesFromFileMessages(File fileMessages) throws IOException {
        return Files.readAllLines(fileMessages.toPath());


    }

    protected boolean isVisibleLine(String messageLine) {

        try {
            if (!Strings.isNullOrEmpty(validateMessage(messageLine))) {
                return DateTime.now().getMillis() >
                        ObjectUtils.defaultIfNull(
                                Longs.tryParse(getMessageElement(messageLine, 1)),
                                0L);
            }
        } catch (NoSuchElementException nee) {
            LOG.error("An exception occurred while extracting visible status from message line '" + messageLine + "'", nee);
        }
        return false;
    }

    private String getMessageElement(String messageLine, int position) {
        return Iterables.get(Splitter.on(":").split(validateMessage(messageLine)), position);
    }

    protected String validateMessage(String messageLine) {

        if (Strings.isNullOrEmpty(messageLine) || Splitter.on(":").splitToList(messageLine).size() != 5) {
            throw new IllegalArgumentException("message line invalid '" + messageLine + "'");
        }
        return messageLine;
    }


    protected String udpateMessageVisibility(String messageLine, Integer delaySeconds) {

        List<String> recordFields = Lists.newArrayList(Splitter.on(":").split(validateMessage(messageLine)));

        long visibility = DateTime.now().getMillis() + TimeUnit.SECONDS.toMillis(delaySeconds);

        return Joiner.on(":").useForNull("").join(recordFields.get(0), visibility, recordFields.get(2), recordFields.get
                (3), recordFields.get(4));
    }

    protected void writeNewVisibilityToFile(Supplier<Stream<String>> streamSupplier, PrintWriter writer, String
            visibleLineToWrite) {


        final String messageId = retrieveMessageId(visibleLineToWrite).orElseThrow(() -> new IllegalStateException("no " +
                "message identifier found for record '" + visibleLineToWrite + "'"));
        streamSupplier.get().forEach(s -> {
            if (s.contains(messageId)) {
                s = visibleLineToWrite;
            }
            writer.println(s);
        });

    }

    protected Optional<String> retrieveMessageId(String messageLine) {

        Optional<String> messageId = Optional.empty();

        if (!Strings.isNullOrEmpty(validateMessage(messageLine))) {
            messageId = Optional.ofNullable(getMessagesElement(messageLine, 3));
        }

        return messageId;

    }

    protected String updateMessageVisibility(String messageLine, Integer delaySeconds) {
        List<String> recordFields = Lists.newArrayList(Splitter.on(":").split(
                validateMessage(messageLine)));

        long visibility = DateTime.now().getMillis() + TimeUnit.SECONDS.toMillis(delaySeconds);

        return Joiner.on(":")
                .useForNull("")
                .join(recordFields.get(0), visibility, recordFields.get(2), recordFields.get(3), recordFields.get(4));
    }


    protected class FileLockShutdownHook implements Runnable {

        public void run() {
            if (!CollectionUtils.isEmpty(lockFiles)) {
                lockFiles.stream().map(File::delete);
                lockFiles.clear();
            }
        }
    }


    private String getMessagesElement(String messageLine, int position) {

        return Iterables.get(Splitter.on(":").split(validateMessage(messageLine)), position);
    }


    protected void replaceWithNewFile(File fileMessages, File newFileMessages) throws IOException {

        requireNonNull(fileMessages, "Messages file must not be null");
        requireNonNull(newFileMessages, "newFileMessages file must not be null");

        Files.deleteIfExists(fileMessages.toPath());

        newFileMessages.renameTo(new File(fileMessages.getPath()));

        Files.deleteIfExists(new File(newFileMessages.getPath()).toPath());


    }

    protected File getMessagesFile(String queueName) {
        checkArgument(!Strings.isNullOrEmpty(queueName), "queueName must not be null");

        // create queue directory
        createDirectory(canvaDirPath + File.separator + queueName);

        // create message file
        return createFile(new File(canvaDirPath + File.separator + queueName + File.separator + MESSAGES_FILE_NAME));
    }

    protected void writeLinesToNewFile(File newFileMessages, List<String> linesWithoutReceiptHandle) throws IOException {
        Files.write(newFileMessages.toPath(), linesWithoutReceiptHandle);
    }

}
