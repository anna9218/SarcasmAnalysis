import com.example.ec2.CreateInstance;
import com.example.ec2.DescribeInstances;
import com.example.ec2.SendReceiveMessages;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
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

import java.io.*;
import java.util.*;

public class Main {

    /**
     * String constant for the AMI Id
     */
    public static final String AMI_ID = "ami-04e48c51b9607511e";
    public static final String terminateQueueName = "terminate";
    public static Tag managerTag = Tag.builder().key("Manager").value("Manager").build();
    public static String bucket = "dsps212apps";
    public static String inputFilesQueueName = "input";

    /**
     * Responsible for proccessing the output and create an html output file.
     * @param outputFileName is the name of the putput file, received as argument at start.
     * @param resArray is in json format, containing the output.
     */
    public static void processResult(String outputFileName, JSONArray resArray) {
        HTMLHandler htmlHandler = new HTMLHandler(outputFileName);
        for (Object o : resArray) {
            JSONObject jsonObj = (JSONObject) o;
            String title = (String)jsonObj.get("title");
            JSONArray reviewsList = (JSONArray)jsonObj.get("reviews");
            try {
                htmlHandler.addTitleAndReviews(title, reviewsList);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        htmlHandler.removeLastNextTag();
        try {
            htmlHandler.writeToHtml(); // and write html file
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        System.out.println("Starting Local Application Process");
        JSONParser parser = new JSONParser();
        // handling args if should terminate or not
        boolean terminate = false;
        int endIndex = args.length;
        if (Arrays.asList(args).contains("[terminate]")) {
            terminate = true;
            endIndex--;
        }
        // the n will indicate the workers ratio - will pass to manager process args while bootstrapping
        int  n = Integer.parseInt(args[endIndex - 1]);
        int inputOutputLength = endIndex - 1;
        int inputLength = inputOutputLength / 2;

        HashMap<String, String> inputOutputMap = new HashMap<>();   // to map between input and output file names
        List<String> inputsToSend = new LinkedList<>();
        String uniqueID = UUID.randomUUID().toString(); // create random name for output queue

        String outputQueueName = "output" + uniqueID;

        // map between input and output file names
        for(int i = 0; i < inputLength; i++) {
            inputOutputMap.put(args[i], args[i + inputLength]);
            inputsToSend.add(args[i]);
        }

        // trying to parse the aws credentials - they are stores in a json format file
        Object obj = null;
        try {
            obj = parser.parse(new FileReader("credentials.json"));
        } catch (IOException | ParseException e) {
            e.printStackTrace();
        }

        // A JSON object. Key value pairs are unordered. JSONObject supports java.util.Map interface.
        JSONObject credentials = (JSONObject) obj;

        // handling credentials for AWS
        AwsSessionCredentials awsCreds =  AwsSessionCredentials.create((String)credentials.get("aws_access_key_id"),
                (String)credentials.get("aws_secret_access_key"),
                (String)credentials.get("aws_session_token"));
                Ec2Client ec2 = Ec2Client.builder().
                region(Region.US_EAST_1).
                credentialsProvider(StaticCredentialsProvider.create(awsCreds)).
                build();
        System.out.println("connection to EC2 Established!");

        // Establish Connection To S3 - storage
        S3Client s3 = S3Client.builder().
                region(Region.US_EAST_1).
                credentialsProvider(StaticCredentialsProvider.create(awsCreds)).
                build();
        System.out.println("connection to S3 Established!");

        // Establish Connection To SQS - service object for communication
        SqsClient sqs = SqsClient.builder().
                region(Region.US_EAST_1).
                credentialsProvider(StaticCredentialsProvider.create(awsCreds)).
                build();
        System.out.println("connection to SQS Established!");

        //creating bucket in s3
        //S3ObjectOperations.createBucket(s3, bucket);

        // create queues - for input, output and terminate
        SendReceiveMessages.createQueue(sqs, inputFilesQueueName);
        SendReceiveMessages.createQueue(sqs, outputQueueName);
        SendReceiveMessages.createQueue(sqs, terminateQueueName);

        // get queue urls
        String inputFilesQueue = SendReceiveMessages.receiveQueueUrl(sqs, inputFilesQueueName);
        String outputQueue = SendReceiveMessages.receiveQueueUrl(sqs, outputQueueName);

        while (!inputOutputMap.isEmpty()){
            // checking if manager instance is running - if not, will create it
            if (!DescribeInstances.isInstanceRunning(ec2, managerTag)) {
                System.out.println("Manager Instance is not Running Will Create it ... ");
                CreateInstance.create(ec2, InstanceType.M4_LARGE, "Manager", "Manager", AMI_ID, n);
            }

            // creating the messages to be sent to workers
            for (String inputFile : inputsToSend) {
                try {
                    File file = new File(inputFile);
                    byte[] bytes = new byte[(int) file.length()];

                    FileInputStream fis = null;
                    try {
                        fis = new FileInputStream(file);
                        // read file into bytes[]
                        fis.read(bytes);
                    } finally {
                        if (fis != null) {
                            fis.close();
                        }
                    }
                    String url = uniqueID + '/' + inputFile + "/input/" + inputFile;
                    // Put Object
                    s3.putObject(PutObjectRequest.builder()
                                    .bucket(bucket).key(url)
                                    .build(),
                            RequestBody.fromBytes(bytes));

                    Map<String, MessageAttributeValue> messageAttributes = new HashMap<>();
                    MessageAttributeValue val1 = MessageAttributeValue.builder()
                            .dataType("String")
                            .stringValue(uniqueID).build();
                    messageAttributes.put("appID", val1);
                    MessageAttributeValue val2 = MessageAttributeValue.builder()
                            .dataType("String")
                            .stringValue(inputFile).build();
                    messageAttributes.put("inputFile", val2);

                    // adding msg to the queue - need to specify queue's url
                    SendMessageRequest send_msg_request = SendMessageRequest.builder()
                            .queueUrl(inputFilesQueue)
                            .messageBody(url)
                            .messageAttributes(messageAttributes)
                            .build();
                    sqs.sendMessage(send_msg_request);
                    System.out.printf("Message: %s, got sent on queue: %s\n", inputFile, inputFilesQueue);
                }
                catch (Exception e){
                    e.printStackTrace();
                }
            }
            inputsToSend = new LinkedList<>(); // will ensure input files sent only one time on queue

            // receive messages from the summary queue to indicate job is done
            ReceiveMessageRequest receiveRequest = ReceiveMessageRequest.builder()
                    .queueUrl(outputQueue)
                    .messageAttributeNames("inputFileName")
                    .build();
            List<Message> messages = sqs.receiveMessage(receiveRequest).messages();

            if (!messages.isEmpty()) {
                for (Message m : messages) {
                    // delete the message from the queue
                    DeleteMessageRequest deleteRequest = DeleteMessageRequest.builder()
                            .queueUrl(outputQueue)
                            .receiptHandle(m.receiptHandle())
                            .build();
                    sqs.deleteMessage(deleteRequest);


                    // Get Object
                    ResponseInputStream<GetObjectResponse> s3objectResponse  = s3.getObject(GetObjectRequest.builder().
                            bucket(bucket).
                            key(m.body()).
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
                        JSONArray jsonArrayResults = (JSONArray) parser.parse(jsonStream.toString());
                        Map<String, MessageAttributeValue> msgAttr = m.messageAttributes();
                        MessageAttributeValue inputFileNameAttr = msgAttr.get("inputFileName");
                        String inputFileName = inputFileNameAttr.stringValue();
                        String outputName = inputOutputMap.get(inputFileName);
                        System.out.printf("Received JSON Array with the results for input file : %s\n", inputFileName);
                        processResult(outputName,jsonArrayResults);
                        inputOutputMap.remove(inputFileName); // remove input output from map
                        System.out.printf("finish dealing with input %s\n", inputFileName);
                    } catch (ParseException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
        System.out.println("finished dealing with all input files");
        // locate summary report and generate html

        // before ending the program send termination message to manager on the connection queue (if terminate signal was received)
        if (terminate) {
            System.out.println("terminating manager");
            String terminateQueue = SendReceiveMessages.receiveQueueUrl(sqs, terminateQueueName);
            SendMessageRequest send_msg_request = SendMessageRequest.builder()
                    .queueUrl(terminateQueue)
                    .messageBody("terminate")
                    .build();
            sqs.sendMessage(send_msg_request);
        }

        // Deleting Local App Queues
        SendReceiveMessages.deleteQueue(sqs, outputQueue);
    }
}
