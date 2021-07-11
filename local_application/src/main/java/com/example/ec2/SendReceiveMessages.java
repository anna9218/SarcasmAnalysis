package com.example.ec2;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;


// snippet-end:[sqs.java2.send_recieve_messages.import]
// snippet-start:[sqs.java2.send_recieve_messages.main]
public class SendReceiveMessages
{

    public static void createQueue(SqsClient sqs, String queueName) {
        // trying to build a queue
        try {
            CreateQueueRequest request = CreateQueueRequest.builder()
                    .queueName(queueName)
                    .build();
            CreateQueueResponse create_result = sqs.createQueue(request);
            System.out.printf("Queue %s Created Successfully in SQS\n", queueName);
        } catch (QueueNameExistsException e) {
            throw e;
        }
    }
    public static void deleteQueue(SqsClient sqs, String queueUrl) {
        // trying to build a queue
        try {
            DeleteQueueRequest request = DeleteQueueRequest.builder()
                    .queueUrl(queueUrl)
                    .build();
            DeleteQueueResponse create_result = sqs.deleteQueue(request);
            System.out.printf("Queue %s Deleted Successfully from SQS\n", queueUrl);
        } catch (QueueNameExistsException e) {
            throw e;
        }
    }

    public static String receiveQueueUrl(SqsClient sqs, String queueName) {
        GetQueueUrlRequest getQueueRequest = GetQueueUrlRequest.builder()
                .queueName(queueName)
                .build();
        String queueUrl = sqs.getQueueUrl(getQueueRequest).queueUrl();
        System.out.printf("fetched %s, queueUrl: %s\n", queueName, queueUrl);
        return queueUrl;
    }


}