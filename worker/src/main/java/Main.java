import com.example.ec2.*;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Main {

    public static String resultsQueueName = "results_ready";
    public static String jobQueueName = "jobs_queue";
    public static Worker worker = new Worker("worker");

    public static void handleJob(SqsClient sqs, String jobQueue,  String resultsQueue, Message msg){
        // perform sentiment analysis and find the named entities
        int sentiment = worker.findSentiment(msg.body());

        List<String> entities = worker.returnEntities(msg.body());

        StringBuilder entitiesString = new StringBuilder();

        for(String entity: entities) {
            entitiesString.append(entity).append(", ");
        }
        try {
            MessageAttributeValue inputUrl = msg.messageAttributes().get("inputFileUrl");
            MessageAttributeValue title = msg.messageAttributes().get("title");
            MessageAttributeValue review_id = msg.messageAttributes().get("review_id");
            MessageAttributeValue appID = msg.messageAttributes().get("appID");
            Map<String, MessageAttributeValue> newMsgAttr = new HashMap<>();

            // construct msg attributes and send the msg to the manager
            newMsgAttr.put("sentiment", MessageAttributeValue.builder()
                    .dataType("Number")
                    .stringValue(String.valueOf(sentiment)).build());
            String finalEntities = " ";
            if (!entities.isEmpty()) {
                finalEntities = entitiesString.toString();
            }

            finalEntities = finalEntities.replaceAll(", $", "");

            newMsgAttr.put("entities", MessageAttributeValue.builder()
                    .dataType("String")
                    .stringValue(finalEntities).build());
            newMsgAttr.put("inputFileUrl", MessageAttributeValue.builder()
                    .dataType("String")
                    .stringValue(inputUrl.stringValue()).build());
            newMsgAttr.put("title", MessageAttributeValue.builder()
                    .dataType("String")
                    .stringValue(title.stringValue()).build());
            newMsgAttr.put("review_id", MessageAttributeValue.builder()
                    .dataType("String")
                    .stringValue(review_id.stringValue()).build());
            newMsgAttr.put("appID", MessageAttributeValue.builder()
                    .dataType("String")
                    .stringValue(appID.stringValue()).build());

            //send the sentiment results back on the results queue
            SendMessageRequest send_msg_request = SendMessageRequest.builder()
                    .queueUrl(resultsQueue)
                    .messageBody(msg.body())
                    .messageAttributes(newMsgAttr)
                    .build();

            System.out.printf("Handled Message and sent results: %s\n", msg.body());
            SendMessageResponse response = sqs.sendMessage(send_msg_request);
            if (response.sdkHttpResponse().isSuccessful()) {
                //delete the message from the queue
                DeleteMessageRequest deleteRequest = DeleteMessageRequest.builder()
                        .queueUrl(jobQueue)
                        .receiptHandle(msg.receiptHandle())
                        .build();
                sqs.deleteMessage(deleteRequest);
            }
        }
        catch (Exception e){
            System.out.println(e.getMessage());
        }
    }


    public static void main(String[] args) {
        System.out.println("Starting Worker Process");

        AwsSessionCredentials awsCreds =  AwsSessionCredentials.create("ASIARQS4I5U7P33NOIQC",
                "ncYrt0UaJ1zXuITc7gUItmCl22T/IZzA5clgW7Vn",
                "IQoJb3JpZ2luX2VjEIX//////////wEaCXVzLXdlc3QtMiJHMEUCICW52HqWOboyNz9Xjzu3emrha3y0Tnf04eE2Ytw5RfJ1AiEA60ThWZGNjox2NRCLO1VviZRv18f/uS7g4/1XY3QG19gqtwII7v//////////ARAAGgwxMDQzNDY0Nzk5MzQiDH4KfPe+Uj44kOzmwCqLAt/eFO3h9Ma2kz/PVwJxSq4/4T46t+/rI6f8cOXoAuJwHgkbQ03M+8VcUi5u39lzyFTag1lOQqv61kD2rbQRvJvFryoGhWk7nA/al/VNvpzjitVyad3fMP7T7kc43bzNAbat9FjJxQCwCQusuidfnWlK6CeQtKXRdoi/vXXqX9KlzO6+i5nbu9x+cUZ+Wzq/lOinD4dvHthPU7e1WBNmCullWWXkhyimxRy3rN8V5ZfXWuJL4okj3rHvEtLxBTLI+3R0erzS5IcYiSS3MDj18v1nyoFFJ2pNwfxlYeho3tnT4iA0E9tBhAZKRTQgWbiPleFh3QOzv4GwJ1J7RiCz2XsObV1+/mocK7d6QzDZpJCEBjqdAQMCLJh35oD6B0+xuYACe6FQ0ClozTcCCvmAVQP6U4meMYDOmQq5KPJclqWIqiy2DvQu3BvNnvS4KGny+Lev9a0ENX1BzmRGVcU5xItcpIPxhvKEUAX5/tCgInz4cFvPjclhrHFUp/C4GGtoRmL9doaoY/YVfG3yS623OvWaWfHEpfxdznFgToH4+p/82QfSECEKp39LqULnt2Ucu2E=");

        // Establish Connection To SQS -service object for communication
        SqsClient sqs = SqsClient.builder().
                region(Region.US_EAST_1).
                credentialsProvider(StaticCredentialsProvider.create(awsCreds)).
                build();
        System.out.println("connection to SQS Established!");


        // get queues urls - results and jobs queues
        String resultsQueue = SendReceiveMessages.receiveQueueUrl(sqs, resultsQueueName);
        String jobQueue = SendReceiveMessages.receiveQueueUrl(sqs, jobQueueName);

        // iterate over all job messages
        while(true) {
            // receive messages from the summary queue to indicate job is done
            ReceiveMessageRequest receiveRequest = ReceiveMessageRequest.builder()
                    .queueUrl(jobQueue)
                    .messageAttributeNames("review_id", "appID", "title", "inputFileUrl")
                    .build();

            List<Message> messages = sqs.receiveMessage(receiveRequest).messages();
            // we assume the msg is the review
            for (Message m : messages) {
                handleJob(sqs, jobQueue, resultsQueue,m);
            }
        }
    }
}
