# Sentiment Analysis
A real-world application to distributively process a list of Amazon reviews, perform sentiment analysis and named-entity recognition and display the result on a web page.

## How to run the project
Each part of the system (Manager, Worker, Local App) is a maven project by itself.
In order to create JAR executables for each project, you would need to run 'mvn clean package' in the corresponding project directory.
In order to work with AWS, you would need to use credentials in order to connect to the console. Then, you should upload the JARs to a bucket "dsps212apps" in S3. 
At last, run the local application with the specified arguments for input and output files, N-ratio, and optional terminate signal. 


## How it works
### Local Application
A local application (aka client) that resides on a local (non-cloud) machine. The application is responsible for reading the input from the user. The input includes the input and output files name, parameter N which stands for the number of workers, and an optional parameter [terminate] which indicates if the process should be terminated.
After receiving and reading the input, the application constantly checks if a Manager node is active on the EC2 cloud. Otherwise, the application starts a Manager node.
In order to initialize a connection to the AWS services, the local app has to use the credentials. First, establish a connection to S3 and SQS. Then create the relevant queues - a queue for input files, output files, and terminate signal. 
Then, it uploads the input file\s to S3. It sends a message to an SQS queue, indicating the exact location of the input file to be processed.
It checks the SQS queue for a finish message which states that the process is done and the response is ready on S3.
It downloads the summary response file from S3 and creates a corresponding HTML file.

*In case the optional terminate parameter was received, the application sends a message to the Manager in order to terminate all processes.


### Manager
The Manager is running on EC2. The Manager is responsible for establishing a connection and checking the SQS queue for input messages which need to be handled. 
At first, it starts a thread executor with 10 threads. Establishes the connection to EC2 (instances), S3 (storage), and SQS (communication).
Creates queues and gets the corresponding URLs - a terminate queue for passing the terminate signal, a job queue for placing workload for workers, and a results queue for the final output.
Gets the input file queue URL as well.
Starts two threads - a thread that will be listening on the results queue, and a thread that will be listening on the terminate queue.
Once a message has arrived, the Manager downloads the input file from S3 where it was stored (it established a connection to S3 earlier). The Manager creates Worker instances according to a given ratio N - the Manager creates and maintains a worker for every N messages. The Manager distributes the operations to the workers using the SQS queue.
The Manager listens and waits to receive results from the workers. It listens for a terminate signal as well. 
While there's no terminate signal, the Manager receives input messages from the input queue and handles them (by passing messages to workers using a queue). Afterward, he deletes the handled messages from the queue. It also handles the case where two different workers processed the same message and ignores the later message.
If there was a terminate signal, the Manager stops accepting any more input files. However, he still serves the local application that has sent the termination signal. The manager terminates all workers instances when the work on the input files is finished.
The result handling - make sure that the received result was received only once and it is not a double message. If we find out that it is, ignore it, as we've already received it.
When the Manager receives results, it places a summary file on S3 and places a message for the local app to check, which indicated that the summary file is ready.


### Worker
The worker is created by the Manager with respect to the messages amount in the queue (the messages amount indicates whether to create more workers). The worker's instance is terminated when the Manager decides to do so.
The worker's responsibility is to get messages from an SQS queue (listening on it), handling the received job/message, and return the results back to the Manager, using a results queue. The worker also checks if the message was successfully sent to the Manager. If so, the worker deletes the message from the job queue.


### Instance types
ami - ami-04e48c51b9607511e
manager instance type - M4.Large
worker instance type - T2.Large


-------------------------------------------------------------------------------------------------

## Q&A

▪ Did you think for more than 2 minutes about security? Do not send your credentials in plain text!

Used a separate file in a JSON format, that holds the AWS credentials. The application extracts the credentials during its runtime.

▪ What about persistence? What if a node dies? What if a node stalls for a while? Have you taken care of all possible outcomes in the system? Think of more possible issues that might arise from failures. What did you do to solve it? What about broken communications? Be sure to handle all fail-cases!

If a worker node dies, or stalls a message for too long, the Visibility Timeout mechanism takes place:
When a consumer receives and processes a message from a queue, the message still remains in the queue, Amazon doesn't automatically delete the message. Thus, the consumer must delete the message from the queue after receiving and processing it. To prevent other consumers from processing the message again, Amazon SQS sets a visibility timeout, a period of time during which Amazon SQS prevents other consumers from receiving and processing the message (30 seconds).
Additionally, when the Manager receives a result message, it checks whether this message was already received. If so, we conclude that it is a double message, and we ignore it. We do this in order to prevent the case where two different workers handled the same message. 

▪ Threads in your application, when is it a good idea? When is it bad? Invest time to think about threads in your application!

Threads in an application require context switches, which can make the app slower. It is a bad idea to use threads when the machine has a single processor. It may also be a bad idea to use threads that update the same resource, or interact with each other - it is crucial to understand the concurrency flow and ensure thread safety.
Have added threads in the application in order for the Manager to listen for messages and results and thus communicate with the other parts of the system.

▪ Did you manage the termination process? Be sure all is closed once requested!

Implemented the termination process - the local application may decide to send a termination message to the Manager. Once it does, the Manager stops accepting any more input files from other local applications. He continues to serve the local application which sent the termination message. The Manager waits for all workers to finish their jobs and terminates them. Then, it creates a result response and terminates. 

▪ Are all your workers working hard? Or some are slacking? Why?

As the system defined, it is possible that some worker is doing a greater amount of work, while another worker is doing little. The Manager does not delegate jobs to specific workers. The worker node takes their messages from the SQS queue. In order to keep a work balance between the workers, we would need to define a balanced ratio and monitor that each worker is indeed working, but not taking too many jobs compared with other active workers.


▪ Is your manager doing more work than he's supposed to? Have you made sure each part of your system has properly defined tasks? Did you mix their tasks? Don't!

Have defined specific tasks for each part of the system, which שרק specified in detail in the 'How it works' section of this file.
Briefly, here are the tasks of each part:
Manager - Downloads the input file, distribute the work between workers (responsible for workers creation w.r.t the amount of workload)
Worker - Gets a message from the SQS queue, performs the job, returns the result, and removes the processed message from the queue.
Local App - starts the Manager, uploads input file to S3, and sends a notifying message. Checks SQS for a message notifying about the results, and downloads the summary file. It may also send a termination message to the Manager.


▪ Lastly, are you sure you understand what distributed means? Is there anything in your system awaiting another?

A distributed system is one with multiple components located on different machines that communicate and coordinate actions in order to appear as a single coherent system to the end-user. In our case, the Manager, Worker, indeed resides on different instances that are available to us on AWS.
The Manager does listen to input messages and results, however, it does not block it from providing service or continue its tasks.
