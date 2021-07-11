//snippet-sourcedescription:[CreateInstance.java demonstrates how to create an EC2 instance.]
//snippet-keyword:[SDK for Java 2.0]
//snippet-keyword:[Code Sample]
//snippet-service:[ec2]
//snippet-sourcetype:[full-example]
//snippet-sourcedate:[11/02/2020]
//snippet-sourceauthor:[scmacdon]
/*
 * Copyright 2010-2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *  http://aws.amazon.com/apache2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
package com.example.ec2;
// snippet-start:[ec2.java2.create_instance.complete]
 
// snippet-start:[ec2.java2.create_instance.import]
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.*;

import java.util.Base64;
import java.util.function.Consumer;
// snippet-end:[ec2.java2.create_instance.import]
 
/**
 * Creates an EC2 instance
 */
public class CreateInstance {
    public static void create(Ec2Client ec2, InstanceType type, String key, String value, String amiId, int n) {
        String MANAGER_ARN = "arn:aws:iam::104346479934:instance-profile/Manager";
        String USER_DATA_MANAGER = String.format(
                "#!/bin/bash\n" +
                        "sudo mkdir /home/ass/\n" +
                        "sudo aws s3 cp s3://dsps212jar/dsps212Manager.jar /home/ass/\n" +
                        "sudo /usr/bin/java -jar /home/ass/dsps212Manager.jar %d \n" +
                        "shutdown -h now" , n);
		// request to run instances(computers). parameters - instance, image, number of needed computers, userdata [can enter commands as in cmd]
        RunInstancesRequest runRequest = RunInstancesRequest.builder()
                .instanceType(type)
                .imageId(amiId).iamInstanceProfile(IamInstanceProfileSpecification.builder()
                        .arn(MANAGER_ARN).build())
                .userData(Base64.getEncoder().encodeToString(USER_DATA_MANAGER.getBytes()))
                .keyName("key")
                .securityGroups("launch-wizard-2")
                .maxCount(1)
                .minCount(1)
                .build();
 
        RunInstancesResponse response = ec2.runInstances(runRequest);
 
        String instanceId = response.instances().get(0).instanceId();
 
        Tag tag = Tag.builder()
                .key(key)
                .value(value)
                .build();



		// add tag to some computer. [worker/manager tag in order to check the instance]
        CreateTagsRequest tagRequest = CreateTagsRequest.builder()
                .resources(instanceId)
                .tags(tag)
                .build();

        try {
            ec2.createTags(tagRequest);
            System.out.printf("Successfully started EC2 instance %s based on AMI %s\n", instanceId, amiId);

       
        } catch (Ec2Exception e) {
            System.err.println(e.getMessage());
            System.exit(1);
        }
        // snippet-end:[ec2.java2.create_instance.main]
        System.out.println("Done!");
    }
}
// snippet-end:[ec2.java2.create_instance.complete]