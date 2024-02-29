package com.example.CoordinatorServer;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.ec2.model.AllocateAddressRequest;
import com.amazonaws.services.ec2.model.AllocateAddressResult;
import com.amazonaws.services.ec2.model.AssociateAddressRequest;
import com.amazonaws.services.ec2.model.DescribeAddressesResult;
import com.amazonaws.services.ec2.model.DisassociateAddressRequest;
import com.amazonaws.services.ec2.model.TerminateInstancesRequest;
import com.amazonaws.util.EC2MetadataUtils;
import com.amazonaws.services.ec2.model.Address;

import java.util.Collections;
import java.util.HashMap;

import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Service
public class CoordinatorServerService {

    @Value("${aws.access.key}")
    private String awsAccessKey;

    @Value("${aws.secret.key}")
    private String awsSecretKey;

    @Value("${aws.region}")
    private String awsRegion;

    private final AmazonEC2 ec2Client;
    private String elasticIpAddress;
    private String elasticIpAllocationId;
    private String currentLeaderInstanceId;
    private String leaderInstancePrivateIPAddress;

    private final Map<Integer, String> brokerInstanceMap = new HashMap<>();

    public CoordinatorServerService() {
        BasicAWSCredentials awsCredentials = new BasicAWSCredentials(awsAccessKey, awsSecretKey);
        ec2Client = AmazonEC2ClientBuilder.standard()
                .withCredentials(new AWSStaticCredentialsProvider(awsCredentials))
                .withRegion(awsRegion)
                .build();
    }

    public synchronized void registerInstance(String instanceId, int uniqueId) {
        if (brokerInstanceMap.isEmpty()) {// ensuring that generateElasticIP and association happens only once for 1st
                                          // broker's registration
            brokerInstanceMap.put(uniqueId, instanceId);
            generateElasticIp();
            associateElasticIpWithLeader(instanceId);
        } else {
            brokerInstanceMap.put(uniqueId, instanceId);
        }

    }

    public synchronized void unregisterInstance(int uniqueId) {
        String instanceId = brokerInstanceMap.remove(uniqueId);
        if (instanceId != null) {
            if (instanceId.equals(currentLeaderInstanceId)) {
                disassociateElasticIp(instanceId);
            }
            terminateEC2Instance(instanceId);
        }
    }

    public synchronized boolean isInstanceRegistered(String instanceId) {
        return brokerInstanceMap.containsValue(instanceId);
    }

    public synchronized boolean isInstanceLeader(String instanceId) {
        return instanceId.equals(currentLeaderInstanceId);
    }

    public synchronized void generateElasticIp() {
        if (elasticIpAllocationId == null) {
            AllocateAddressRequest allocateRequest = new AllocateAddressRequest();
            AllocateAddressResult allocateResult = ec2Client.allocateAddress(allocateRequest);
            elasticIpAllocationId = allocateResult.getAllocationId();
            setElasticIpAddress(allocateResult.getPublicIp());
        }
    }

    public void setElasticIpAddress(String elasticIpAddress) {
        this.elasticIpAddress = elasticIpAddress;
    }

    public String getElasticIpAddress() {
        if (elasticIpAddress != null) {
            return elasticIpAddress;
        } else
            return null;

    }

    public void setleadEC2BrokerPrivateIP(String leaderInstancePrivateIPAddress) {
        this.leaderInstancePrivateIPAddress = leaderInstancePrivateIPAddress;
    }

    public String getleadEC2BrokerPrivateIP() {
        if (leaderInstancePrivateIPAddress != null) {
            return leaderInstancePrivateIPAddress;
        } else
            return null;
    }

    /*
     * node must be registered and elaticIP must exist for the
     * association. Helps prevent external non-registered EC2
     * instance from being associated with the elasticIPAddress
     */
    public synchronized void associateElasticIpWithLeader(String leaderInstanceId) {
        if (elasticIpAllocationId != null && isInstanceRegistered(leaderInstanceId)) {
            AssociateAddressRequest associateRequest = new AssociateAddressRequest()
                    .withInstanceId(leaderInstanceId)
                    .withAllocationId(elasticIpAllocationId);
            ec2Client.associateAddress(associateRequest);
            currentLeaderInstanceId = leaderInstanceId;
            setleadEC2BrokerPrivateIP(EC2MetadataUtils.getInstanceInfo().getPrivateIp());
        }
    }

    private synchronized void disassociateElasticIp(String instanceId) {

        DescribeAddressesResult describeResult = ec2Client.describeAddresses();
        List<Address> addresses = describeResult.getAddresses();

        for (Address address : addresses) {
            if (address.getInstanceId() != null && address.getInstanceId().equals(instanceId)) {
                DisassociateAddressRequest disassociateRequest = new DisassociateAddressRequest()
                        .withAssociationId(address.getAssociationId());

                ec2Client.disassociateAddress(disassociateRequest);
                break;
            }
        }

        // if there are 0 EC2 broker nodes active, the elastic IP address should not be
        // available for publisher and subscriber.
        // After EC2 broker nodes become 0, when a new EC2 broker node registers, a new
        // a elasticIP address value will be generated.
        // So, the below function sets the elasticIPAddress and
        // leaderInstancePrivateIPAddress to null

        if (brokerInstanceMap.isEmpty()) {
            this.elasticIpAddress = null;
            this.leaderInstancePrivateIPAddress = null;
        }

    }

    private synchronized void electNewLeaderAndAssociateElasticIp() {
        if (!brokerInstanceMap.isEmpty()) {
            int maxUniqueId = Collections.max(brokerInstanceMap.keySet());
            String newLeaderInstanceId = brokerInstanceMap.get(maxUniqueId);
            associateElasticIpWithLeader(newLeaderInstanceId);
        } else {
            System.out.println("Broker cluster is no longer in service.");
        }
    }

    private void terminateEC2Instance(String instanceId) {
        TerminateInstancesRequest terminateRequest = new TerminateInstancesRequest().withInstanceIds(instanceId);
        ec2Client.terminateInstances(terminateRequest);
    }

}
