import com.example.ec2.*;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.InstanceType;
import software.amazon.awssdk.services.ec2.model.Tag;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;


public class Main {
    public static ConcurrentHashMap<String, Vector<InputFileRequest>> appToInputFileRequests= new ConcurrentHashMap<>();
    public static AtomicInteger runningWorkers = new AtomicInteger(0);
    public static boolean TERMINATE_SIGNAL = false;
    public static boolean TERMINATE = false;
    public static String bucket = "dsps212apps";
    public static String terminateQueueName = "terminate";
    public static String inputFileQueueName = "input";
    public static String jobQueueName = "jobs_queue";
    public static String resultsQueueName = "results_ready";
    public static Tag workerTag = Tag.builder().key("Worker").value("Worker").build();

    /**
     * String constant for the AMI Id
     */
    public static final String AMI_ID = "ami-04e48c51b9607511e";

    /**
     * Divides and handles input reviews
     * @param fileString is the complete input String
     * @return the divided output
     */
    public static List<String> splitBalance(String fileString) {

        int currIndex = 0;
        int closing = 0;
        int opening = 0;
        List<String> output = new LinkedList<>();
        for (int i = 0; i < fileString.length(); i++){
            char c = fileString.charAt(i);
            if (c == '{') {
                opening++;
            }
            else if (c == '}') {
                closing ++;
            }
            if (opening == closing){
                output.add(fileString.substring(currIndex, i + 1));
                currIndex = i + 1;
            }
        }
        return output;
    }

    public static synchronized void creatAppInputVector(String appID) {
        appToInputFileRequests.computeIfAbsent(appID, k -> new Vector<>());
    }

    public static InputFileRequest createInputRequest(S3Client s3, String appID, String inputFile,  String inputFileUrl) {
        JSONParser parser = new JSONParser();
        // Get Object
        ResponseInputStream<GetObjectResponse> s3objectResponse  = s3.getObject(GetObjectRequest.builder().
                bucket(bucket).
                key(inputFileUrl).
                build());
        BufferedReader reader = new BufferedReader(new InputStreamReader(s3objectResponse));

        String line;
        StringBuilder jsonStream = new StringBuilder();
        try {
            while ((line = reader.readLine()) != null) {
                jsonStream.append(line);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            ConcurrentHashMap<String, TitleReviewRequest> titleRequests = new ConcurrentHashMap<>();
            int msgCount = 0;
            String allJsonFile = jsonStream.toString();
            List<String> jsonStringList = splitBalance(allJsonFile);
            for (String jsonString : jsonStringList) {
                JSONObject jsonObject = (JSONObject) parser.parse(jsonString);
                String title = (String) jsonObject.get("title");
                JSONArray reviews = (JSONArray) jsonObject.get("reviews");
                TitleReviewRequest titleRequest = new TitleReviewRequest(title, reviews);
                if (titleRequests.get(title) != null) {
                    TitleReviewRequest existed = titleRequests.get(title);
                    existed.addReviews(reviews);
                }
                else {
                    titleRequests.put(title, titleRequest);
                }
                msgCount += reviews.size();
            }
            InputFileRequest request = new InputFileRequest(appID, inputFile, inputFileUrl,titleRequests);
            request.msgCount.set(msgCount);
            return request;
        }
        catch (Exception e) {
            System.out.println(e.getMessage());
            return null;
        }

    }

    public static synchronized void checkAndRunWorkers(Ec2Client ec2, SqsClient sqs, String jobQueue, int n) {
        /**
         * Create/add workers according to the messages amount
         */
        QueueAttributeName countCommand = QueueAttributeName.fromValue("ApproximateNumberOfMessages");
        GetQueueAttributesRequest requestCount = GetQueueAttributesRequest.builder().queueUrl(jobQueue).attributeNames(countCommand).build();
        GetQueueAttributesResponse response = sqs.getQueueAttributes(requestCount);
        Map<QueueAttributeName, String> attr = response.attributes();
        int count = Integer.parseInt(attr.get(countCommand));
        int required = count / n ;
        int runningNow = DescribeInstances.countInstanceRunning(ec2, workerTag);
        runningWorkers.set(runningNow);
        System.out.printf("Current Running Workers: %d\n", runningNow);
        int needToRun = required - runningNow;
        if (needToRun > 0) {
            System.out.printf("Need To Run  %d More Workers:\n", needToRun);
            if(runningWorkers.get() + needToRun >= 18){
                System.out.println("Required exceed worker count limits");
                int addUpToLimit = 18 - runningWorkers.get();
                if (addUpToLimit > 0) {
                    for (int i = 0; i < addUpToLimit; i++) {
                        CreateInstance.create(ec2, InstanceType.T2_LARGE, "Worker", "Worker", AMI_ID);
                        runningWorkers.incrementAndGet();
                    }
                }
            }
            else {
                for (int i = 0; i < needToRun; i++) {
                    CreateInstance.create(ec2, InstanceType.T2_LARGE, "Worker", "Worker", AMI_ID);
                    runningWorkers.incrementAndGet();
                }
            }
        }
    }


    public static void handleInputFileRequest(Ec2Client ec2 ,SqsClient sqs, S3Client s3, String jobQueue, Message msg, int n) {


        // connection msg from local app establish
        Map<String, MessageAttributeValue> msgAttr = msg.messageAttributes();
        MessageAttributeValue appID = msgAttr.get("appID");
        MessageAttributeValue inputFile = msgAttr.get("inputFile");
        String inputFileUrl = msg.body();

        creatAppInputVector(appID.stringValue()); // verify that vector requests init
        Vector<InputFileRequest> requests = appToInputFileRequests.get(appID.stringValue());

        InputFileRequest request = createInputRequest(s3,appID.stringValue(), inputFile.stringValue(), inputFileUrl);
        requests.add(request);
        System.out.printf("Handling Request of Local App: %s, Input File Url: %s\n",appID.stringValue(), inputFileUrl);

        assert request != null;
        request.setStatus(Status.PROCESSING);
        ConcurrentHashMap<String, TitleReviewRequest> titleRequests = request.getTitleReviewRequests();

        for(TitleReviewRequest titleRequest : titleRequests.values()) {
            ConcurrentHashMap<String, ReviewRequest> reviews = titleRequest.reviewRequests;
            for (ReviewRequest review : reviews.values()) {

                Map<String, MessageAttributeValue> messageAttributes = new HashMap<>();
                messageAttributes.put("review_id", MessageAttributeValue.builder()
                        .dataType("String")
                        .stringValue(review.review_id).build());
                messageAttributes.put("title", MessageAttributeValue.builder()
                        .dataType("String")
                        .stringValue(titleRequest.title).build());
                messageAttributes.put("inputFileUrl", MessageAttributeValue.builder()
                        .dataType("String")
                        .stringValue(inputFileUrl).build());
                messageAttributes.put("appID", MessageAttributeValue.builder()
                        .dataType("String")
                        .stringValue(appID.stringValue()).build());

                // adding msg to the queue - need to specify queue's url
                SendMessageRequest send_msg_request = SendMessageRequest.builder()
                        .queueUrl(jobQueue)
                        .messageBody(review.text)
                        .messageAttributes(messageAttributes)
                        .build();

                try {
                    sqs.sendMessage(send_msg_request);
                }
                catch (Exception e){
                    System.out.println(e.getMessage());
                }

            }
        }

        checkAndRunWorkers(ec2, sqs,jobQueue, n);
        // need to understand how many worker instances to activate
    }

    public static InputFileRequest getInputFileRequest(String inputFileUrl, Vector<InputFileRequest> appInputFileRequests){
        for(InputFileRequest request: appInputFileRequests) {
            if (request.inputUrl.equals(inputFileUrl)) {
                return request;
            }
        }
        return null;
    }

    @SuppressWarnings("unchecked")
    public static void handleResult(SqsClient sqs, S3Client s3, String resultsQueue, Message m) {
        //delete the message from the queue
        DeleteMessageRequest deleteRequest = DeleteMessageRequest.builder()
                .queueUrl(resultsQueue)
                .receiptHandle(m.receiptHandle())
                .build();

        sqs.deleteMessage(deleteRequest);

        Map<String, MessageAttributeValue> msgAttr = m.messageAttributes();
        MessageAttributeValue sentiment = msgAttr.get("sentiment");
        MessageAttributeValue entities = msgAttr.get("entities");
        MessageAttributeValue inputFileUrl = msgAttr.get("inputFileUrl");
        MessageAttributeValue title = msgAttr.get("title");
        MessageAttributeValue review_id = msgAttr.get("review_id");
        MessageAttributeValue appID = msgAttr.get("appID");
        String inputFileUrlString = inputFileUrl.stringValue();
        Vector<InputFileRequest> appInputFileRequests = appToInputFileRequests.get(appID.stringValue());
        InputFileRequest request = getInputFileRequest(inputFileUrl.stringValue(), appInputFileRequests);
        assert request != null;
        TitleReviewRequest titleRequest = request.getTitleReviewRequest(title.stringValue());
        ReviewRequest reviewRequest = titleRequest.getReviewRequest(review_id.stringValue());
        if(reviewRequest.status == Status.DONE){
            System.out.println("received duplicate message that already processed, will ignore...");

        }
        else {
            System.out.println("MESSAGE RECEIVED: \n");
            System.out.printf("APP ID: %s\n", appID.stringValue());
            System.out.printf("Sentiment Result: %s\n", sentiment.stringValue());
            System.out.printf("Entities: %s\n", entities.stringValue());
            System.out.println(m.body());

            JSONObject review = reviewRequest.summeryJson;
            review.put("review_id", review_id.stringValue());
            review.put("text", m.body());
            review.put("sentiment", sentiment.stringValue());
            review.put("entities", entities.stringValue());
            reviewRequest.status = Status.DONE;

            int msgCount= request.msgCount.get();
            System.out.printf("Countdown Msg Count: %d\n", msgCount);
            msgCount = request.msgCount.decrementAndGet();
            if (msgCount == 0) {
                // remove infile from map
                request.prepare();
                int lastSlash = inputFileUrlString.lastIndexOf("/");
                String outputUrl = inputFileUrlString.substring(0, lastSlash) + "/output.json";
                // Put Object
                s3.putObject(PutObjectRequest.builder()
                                .bucket(bucket).key(outputUrl)
                                .build(),
                        RequestBody.fromBytes(request.summeryFile.toString().getBytes(StandardCharsets.UTF_8)));
                String appQueueName = "output" + appID.stringValue();
                String appQueue = SendReceiveMessages.receiveQueueUrl(sqs, appQueueName);
                // adding msg to the queue - need to specify queue's url
                Map<String, MessageAttributeValue> messageAttributes = new HashMap<>();
                messageAttributes.put("inputFileName", MessageAttributeValue.builder()
                        .dataType("String")
                        .stringValue(request.inputFileName).build());

                SendMessageRequest send_msg_request = SendMessageRequest.builder()
                        .queueUrl(appQueue)
                        .messageBody(outputUrl)
                        .messageAttributes(messageAttributes)
                        .build();
                sqs.sendMessage(send_msg_request);

                appInputFileRequests.remove(request);
                if(appInputFileRequests.isEmpty()){
                    appToInputFileRequests.remove(appID.stringValue(), appInputFileRequests);
                }
            }
        }
    }

    public static void listenForResults(SqsClient sqs, S3Client s3, String resultsQueue){
        int nThread = 10;
        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(nThread);
        while (!TERMINATE || !appToInputFileRequests.isEmpty()) {
            // receive messages from the input queue
            ReceiveMessageRequest receiveRequest = ReceiveMessageRequest.builder()
                    .queueUrl(resultsQueue)
                    .messageAttributeNames("appID", "sentiment", "entities", "review_id", "title", "inputFileUrl")
                    .build();
            List<Message> messages = sqs.receiveMessage(receiveRequest).messages();
            //listen to input file requests and let threatened handle them
            for (Message m : messages) {
                executor.submit(() -> {
                    handleResult(sqs, s3, resultsQueue, m);
                });
            }
        }
        executor.shutdown();
    }

    public static void listenForTerminate (SqsClient sqs, String terminateQueue) {
        while (!TERMINATE) {
            // receive messages from the input queue
            ReceiveMessageRequest receiveRequest = ReceiveMessageRequest.builder()
                    .queueUrl(terminateQueue)
                    .build();
            List<Message> messages = sqs.receiveMessage(receiveRequest).messages();
            //listen to input file requests and let threatened handle them
            for (Message m : messages) {
                TERMINATE_SIGNAL = true;
                //delete the message from the queue
                DeleteMessageRequest deleteRequest = DeleteMessageRequest.builder()
                        .queueUrl(terminateQueue)
                        .receiptHandle(m.receiptHandle())
                        .build();
                sqs.deleteMessage(deleteRequest);
            }
        }
    }

    public static void main(String[] args) {
        System.out.println("Starting Manager Process");
        // will indicate the workers ration
        int n = Integer.parseInt(args[0]);
        int nThread = 10;
        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(nThread);

//        AwsSessionCredentials awsCreds =  AwsSessionCredentials.create("ASIARQS4I5U7P33NOIQC",
//                "ncYrt0UaJ1zXuITc7gUItmCl22T/IZzA5clgW7Vn",
//                "IQoJb3JpZ2luX2VjEIX//////////wEaCXVzLXdlc3QtMiJHMEUCICW52HqWOboyNz9Xjzu3emrha3y0Tnf04eE2Ytw5RfJ1AiEA60ThWZGNjox2NRCLO1VviZRv18f/uS7g4/1XY3QG19gqtwII7v//////////ARAAGgwxMDQzNDY0Nzk5MzQiDH4KfPe+Uj44kOzmwCqLAt/eFO3h9Ma2kz/PVwJxSq4/4T46t+/rI6f8cOXoAuJwHgkbQ03M+8VcUi5u39lzyFTag1lOQqv61kD2rbQRvJvFryoGhWk7nA/al/VNvpzjitVyad3fMP7T7kc43bzNAbat9FjJxQCwCQusuidfnWlK6CeQtKXRdoi/vXXqX9KlzO6+i5nbu9x+cUZ+Wzq/lOinD4dvHthPU7e1WBNmCullWWXkhyimxRy3rN8V5ZfXWuJL4okj3rHvEtLxBTLI+3R0erzS5IcYiSS3MDj18v1nyoFFJ2pNwfxlYeho3tnT4iA0E9tBhAZKRTQgWbiPleFh3QOzv4GwJ1J7RiCz2XsObV1+/mocK7d6QzDZpJCEBjqdAQMCLJh35oD6B0+xuYACe6FQ0ClozTcCCvmAVQP6U4meMYDOmQq5KPJclqWIqiy2DvQu3BvNnvS4KGny+Lev9a0ENX1BzmRGVcU5xItcpIPxhvKEUAX5/tCgInz4cFvPjclhrHFUp/C4GGtoRmL9doaoY/YVfG3yS623OvWaWfHEpfxdznFgToH4+p/82QfSECEKp39LqULnt2Ucu2E=");


        //Establish Connection To EC2 - instances
        Ec2Client ec2 = Ec2Client.builder().
                region(Region.US_EAST_1).
                //credentialsProvider(StaticCredentialsProvider.create(awsCreds)).
                build();
        System.out.println("connection to EC2 Established!");

        //Establish Connection To S3 - storage
        S3Client s3 = S3Client.builder().
                region(Region.US_EAST_1).
                //credentialsProvider(StaticCredentialsProvider.create(awsCreds)).
                build();
        System.out.println("connection to S3 Established!");

        // Establish Connection To SQS -service object for communication
        SqsClient sqs = SqsClient.builder().
                region(Region.US_EAST_1).
                //credentialsProvider(StaticCredentialsProvider.create(awsCreds)).
                build();
        System.out.println("connection to SQS Established!");



        //create queues
        SendReceiveMessages.createQueue(sqs, terminateQueueName);
        SendReceiveMessages.createQueue(sqs, jobQueueName);
        SendReceiveMessages.createQueue(sqs, resultsQueueName);

        // get queue urls
        String inputFileQueue = SendReceiveMessages.receiveQueueUrl(sqs, inputFileQueueName);
        String jobQueue = SendReceiveMessages.receiveQueueUrl(sqs, jobQueueName);
        String resultsQueue = SendReceiveMessages.receiveQueueUrl(sqs, resultsQueueName);
        String terminateQueue = SendReceiveMessages.receiveQueueUrl(sqs, terminateQueueName);

        //delete queues
        //SendReceiveMessages.deleteQueue(sqs, inputFileQueue);
        //SendReceiveMessages.deleteQueue(sqs, jobQueue);
        //SendReceiveMessages.deleteQueue(sqs, resultsQueue);

        Thread resultsListenThread = new Thread(() -> {
            listenForResults(sqs, s3, resultsQueue);
        });
        Thread terminateListenThread = new Thread(() -> {
            listenForTerminate(sqs, terminateQueue);
        });

        // start listening
        resultsListenThread.start();
        terminateListenThread.start();

        while (!TERMINATE_SIGNAL) {
            // constructs a new ReceiveMessageRequest object in order to receive messages from the input queue
            ReceiveMessageRequest receiveRequest = ReceiveMessageRequest.builder()
                    .queueUrl(inputFileQueue)
                    .messageAttributeNames("appID", "inputFile")
                    .build();
            // retrieves one or more messages, from the input queue
            List<Message> messages = sqs.receiveMessage(receiveRequest).messages();
            // listen to input file requests and let thread executor handle them
            for (Message m : messages) {
                executor.execute(() -> {
                    handleInputFileRequest(ec2, sqs, s3, jobQueue, m, n);
                });
                // constructs a new DeleteMessageRequest object in order to delete the message from the queue
                DeleteMessageRequest deleteRequest = DeleteMessageRequest.builder()
                        .queueUrl(inputFileQueue)
                        .receiptHandle(m.receiptHandle())
                        .build();
                // delete the specified message from the input queue
                sqs.deleteMessage(deleteRequest);
            }
        }

        // if a terminate signal was received - the manager should not accept any more input files. still serves the app that sent the termination signal.
        executor.shutdown();
        // signal to result thread listen to shutdown
        TERMINATE = true;

        System.out.println("Manager stopped taking input files");

        try {
            // here should wait for results from workers and terminate listener thread to get signal and shutdown
            System.out.println("Manager waiting for working to finish all pending input files");
            resultsListenThread.join();
            System.out.println("Manager finished working on all input files");
            System.out.println("killing all workers");
            DescribeInstances.kllInstances(ec2, workerTag);
            terminateListenThread.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println("Manager Process End");
    }

}
