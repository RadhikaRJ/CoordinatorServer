package com.example.CoordinatorServer;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.ec2.model.AllocateAddressRequest;
import com.amazonaws.services.ec2.model.AllocateAddressResult;
import com.amazonaws.services.ec2.model.AssociateAddressRequest;
import com.amazonaws.services.ec2.model.DescribeAddressesResult;
import com.amazonaws.services.ec2.model.DescribeInstancesRequest;
import com.amazonaws.services.ec2.model.DescribeInstancesResult;
import com.amazonaws.services.ec2.model.DisassociateAddressRequest;
import com.amazonaws.services.ec2.model.Reservation;
import com.amazonaws.services.ec2.model.TerminateInstancesRequest;
import com.amazonaws.services.ec2.model.Address;
import com.amazonaws.services.ec2.model.Instance;

import java.util.Collections;
import java.util.HashMap;

import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Service
public class CoordinatorServerService {

    private final AmazonEC2 ec2Client;
    private String elasticIpAddress;
    private String elasticIpAllocationId;
    private String currentLeaderInstanceId;
    private String leaderInstancePrivateIPAddress;

    private final Map<Integer, String> brokerInstanceMap = new HashMap<>();

    public CoordinatorServerService(@Value("${aws.accessKeyId}") String awsaccessKeyId,
            @Value("${aws.secretKey}") String awssecretKey) {

        BasicAWSCredentials awsCredentials = new BasicAWSCredentials(awsaccessKeyId, awssecretKey);
        ec2Client = AmazonEC2ClientBuilder.standard()
                .withCredentials(new AWSStaticCredentialsProvider(awsCredentials))
                .build();
        System.out.println("ec2Client initialization is successful.");
    }

    public synchronized void registerInstance(String instanceId, int uniqueId) {
        if (brokerInstanceMap.isEmpty()) {
            // ensuring that generateElasticIP and association happens only once for 1st
            // broker's registration
            brokerInstanceMap.put(uniqueId, instanceId);
            generateElasticIp();
            associateElasticIpWithLeader(instanceId);
        } else {
            brokerInstanceMap.put(uniqueId, instanceId);
        }
        System.out.println(
                "Broker with uniqueId " + uniqueId + " has been successfully registered at Coordinator Server");

    }

    public synchronized void unregisterInstance(int uniqueId) {
        String instanceId = brokerInstanceMap.remove(uniqueId);
        if (instanceId != null) {
            if (instanceId.equals(currentLeaderInstanceId)) {
                disassociateElasticIp(instanceId);

            }
            terminateEC2Instance(instanceId);
            System.out.println("EC2 instance with uniqueId " + uniqueId + " is terminated. ");
        }
        System.out.println(
                "Broker node with UniqueId: " + uniqueId + " has deregistered from coordinator server successfully");
    }

    public synchronized boolean isInstanceRegistered(String instanceId) {
        return brokerInstanceMap.containsValue(instanceId);
    }

    public synchronized boolean isInstanceLeader(String instanceId) {
        return instanceId.equals(currentLeaderInstanceId);
    }

    public synchronized void generateElasticIp() {
        System.out.println("Generating elasticIP address");
        if (elasticIpAllocationId == null) {

            // Allocate a new Elastic IP address if none exists
            AllocateAddressRequest allocateRequest = new AllocateAddressRequest();
            AllocateAddressResult allocateResult = ec2Client.allocateAddress(allocateRequest);
            elasticIpAllocationId = allocateResult.getAllocationId();
            setElasticIpAddress(allocateResult.getPublicIp());
            System.out.println("Elastic IP address has been initialized and allocated.");

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
            return this.leaderInstancePrivateIPAddress;
        } else
            return null;
    }

    public String getCurrentLeaderInstanceId() {
        return this.currentLeaderInstanceId;
    }

    /*
     * Broker EC2 Node must be registered and elaticIP must exist for the
     * association. Helps prevent external non-registered EC2
     * instance from being associated with the AWS Elastic IP Address
     */
    public synchronized void associateElasticIpWithLeader(String leaderInstanceId) {
        if (elasticIpAllocationId != null && isInstanceRegistered(leaderInstanceId)) {
            AssociateAddressRequest associateRequest = new AssociateAddressRequest()
                    .withInstanceId(leaderInstanceId)
                    .withAllocationId(elasticIpAllocationId);
            ec2Client.associateAddress(associateRequest);
            System.out.println("ElasticIP Address has been associated with leader broker node.");
            this.currentLeaderInstanceId = leaderInstanceId;

            // Get the private IP address associated with the leaderInstanceId
            System.out.println("Fetching private IP address of the leader broker node's EC2 instance");
            String leaderPrivateIpAddress = getPrivateIpAddressByInstanceId(leaderInstanceId);

            setleadEC2BrokerPrivateIP(leaderPrivateIpAddress);
            System.out.println("Lead broker node's private IP at coordinator server is updated.");
            System.out.println(
                    "Current leader broker's Private IP address is updated at Coordinator server for new elected lead broker");
        }
    }

    // should now seek public ipv4 address
    private String getPrivateIpAddressByInstanceId(String instanceId) {
        DescribeInstancesRequest request = new DescribeInstancesRequest()
                .withInstanceIds(instanceId);

        DescribeInstancesResult response = ec2Client.describeInstances(request);

        for (Reservation reservation : response.getReservations()) {
            for (Instance instance : reservation.getInstances()) {
                if (instance.getInstanceId().equals(instanceId)) {

                    return instance.getPrivateIpAddress();
                }

            }
        }
        System.out.println("Failed to get a private IP address of elected leader node. Value returned is null.");
        return null; // Return null if instance with the given ID is not found
    }

    private synchronized void disassociateElasticIp(String instanceId) {
        System.out.println("Disassociating ElasticIP address");
        DescribeAddressesResult describeResult = ec2Client.describeAddresses();
        List<Address> addresses = describeResult.getAddresses();

        for (Address address : addresses) {
            if (address.getInstanceId() != null && address.getInstanceId().equals(instanceId)) {
                DisassociateAddressRequest disassociateRequest = new DisassociateAddressRequest()
                        .withAssociationId(address.getAssociationId());

                ec2Client.disassociateAddress(disassociateRequest);
                System.out.println("Elastic IP Address Disassociated");
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
            System.out.println(
                    "No nodes in broker cluster. Setting elasticIP address and leaderInstancePrivateIPAddress to null.");
            this.elasticIpAddress = null;
            this.leaderInstancePrivateIPAddress = null;
        }

    }

    public synchronized void electNewLeaderAndAssociateElasticIp() {
        System.out.println("Leader election is now executing over nodes in broker cluster");
        if (!brokerInstanceMap.isEmpty()) {
            int maxUniqueId = Collections.max(brokerInstanceMap.keySet());
            String newLeaderInstanceId = brokerInstanceMap.get(maxUniqueId);
            System.out.println(
                    "The broker who's UniqueID is the maximum and is registered with Coordinator Server is elected as new lead broker node. Max uniqueID value of elected leader broker nodes is: "
                            + maxUniqueId);
            System.out.println("Associtaing Elastic IP with new elected leader node...");
            associateElasticIpWithLeader(newLeaderInstanceId);
        } else {
            System.out.println("Broker cluster is no longer in service. No broker node to be selected as leader.");
        }
    }

    private void terminateEC2Instance(String instanceId) {
        TerminateInstancesRequest terminateRequest = new TerminateInstancesRequest().withInstanceIds(instanceId);
        ec2Client.terminateInstances(terminateRequest);
    }

}
