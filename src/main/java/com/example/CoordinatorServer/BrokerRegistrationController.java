package com.example.CoordinatorServer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.json.JSONObject;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.HttpEntity;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.RestTemplate;

@RestController
public class BrokerRegistrationController {
    private final CoordinatorServerService coordinatorServerService;
    private final RestTemplate restTemplate = new RestTemplate();
    private final Map<Integer, Broker> brokerRegistry = new HashMap<>();

    @Autowired
    public BrokerRegistrationController(CoordinatorServerService coordinatorServerService) {
        this.coordinatorServerService = coordinatorServerService;
        System.out.println("The Coordinator Server has started execution. \n");

    }

    @GetMapping("/helloConfig")
    public String hello() {
        return "Hello, World! Coordinator service is up & running!";
    }

    @PostMapping("/register-broker")
    public void registerBroker(@RequestBody Broker broker) {
        System.out.println("Existing Broker Registry at Configuration Server: ");
        System.out.println(brokerRegistry);
        brokerRegistry.put(broker.getUniqueId(), broker);
        coordinatorServerService.registerInstance(broker.getEC2instanceID(), broker.getUniqueId());
        System.out.println(
                "Broker node with uniqueID " + broker.getUniqueId() + " is registered with Coordinator Server");
        System.out.println(brokerRegistry);

    }

    @GetMapping("/broker-registry")
    public Map<Integer, Broker> getBrokerRegistry() {
        return brokerRegistry;
    }

    @DeleteMapping("/deregister-broker/{uniqueId}")
    public void deregisterBroker(@PathVariable int uniqueId) {
        Broker broker = brokerRegistry.remove(uniqueId);
        if (broker != null) {
            coordinatorServerService.unregisterInstance(broker.getUniqueId());
            System.out.println("Broker node with uniqueID " + uniqueId + "has deregistered from Coordinator Server.\n");
        }
    }

    @GetMapping("/fetch-leadBroker-elasticIP")
    public String getLeadBrokerPublicIPAddress() {
        System.out.println("Coordinator sending lead broker's (elastic) IP Address. \n");
        return coordinatorServerService.getElasticIpAddress();
    }

    @GetMapping("/getCurrent-leadBroker-PrivateIP")
    public String getCurrentLeadBrokerPrivateIP() {
        System.out.println("Coordinator sending lead broker's private IP Address. \n");
        return coordinatorServerService.getleadEC2BrokerPrivateIP();
    }

    @GetMapping("/get-peerBrokers-IPList")
    public List<String> getPeerBrokerIPList() {
        List<String> peerBrokerIPList = new ArrayList<>();
        String leadEC2BrokerPrivateIP = coordinatorServerService.getleadEC2BrokerPrivateIP();
        for (Broker broker : brokerRegistry.values()) {
            if (!leadEC2BrokerPrivateIP.equals(broker.getIpAddress())) {
                peerBrokerIPList.add(broker.getIpAddress());
            }
        }
        System.out.println(
                "Coordinator Server Requested to send peer broker nodes' private IP addresses. \n Sending the private IP Addresses of peer broker nodes present in the broker cluster\n");
        return peerBrokerIPList;
    }

    private boolean pingLeader(String leaderPrivateIp) {

        boolean isLeaderResponsive = false;
        try {
            System.out.println("Coordinator Server Pinging current leader broker node to check health status...\n");
            String healthCheckUrl = "http://" + leaderPrivateIp + ":8080/leadBroker-status";
            String statusOfLeadBroker = restTemplate.getForObject(healthCheckUrl, String.class);
            System.out.println("Response from current leader broker: " + statusOfLeadBroker);
            isLeaderResponsive = true;
        } catch (Exception e) {
            // Exception occurred, leader is not responsive
            System.err.println("Error occurred while pinging leader broker: " + e.getMessage());
            isLeaderResponsive = false;
            System.out.println(
                    "current lead broker is verified by Coordinator Server to have failed and is not responding \n");
        }

        return isLeaderResponsive;
    }

    public Integer findUniqueIdByIpAddress(String ipAddress) {
        System.out.println("fetching uniqueId associated with IP address \n");
        for (Map.Entry<Integer, Broker> entry : brokerRegistry.entrySet()) {
            if (entry.getValue().getIpAddress().equals(ipAddress)) {
                return entry.getKey();
            }
        }
        return null; // Return null if ipAddress is not found in the brokerRegistry
    }

    public void sendNewLeadBrokerPrivateIPToPeerNodes(List<String> peerBrokerIPAddresses,
            String newLeadBrokerPrivateIPAddress) {
        String endpoint = ":8080/updateLeaderIPAndCheckStatus";
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        System.out.println("Sending the new elected broker's private IP address to nodes in the cluster.\n");
        for (String ipAddress : peerBrokerIPAddresses) {
            JSONObject requestBodyJson = new JSONObject();
            requestBodyJson.put("newLeadBrokerPrivateIPAddress", newLeadBrokerPrivateIPAddress);

            HttpEntity<String> requestEntity = new HttpEntity<>(requestBodyJson.toString(), headers);
            String url = "http://" + ipAddress + endpoint;

            restTemplate.postForObject(url, requestEntity, Void.class);
            System.out.println("new elected leader broker node's IP address sent to: " + ipAddress);
        }
    }

    @PostMapping("/leader-not-responding")
    public void handleLeaderNotResponding(@RequestBody String requestBody) {
        JSONObject jsonObject = new JSONObject(requestBody);
        String currleadBrokerIPAtNode = jsonObject.getString("currleadBrokerIPAtNode");
        System.out.println(
                "leader broker's private IP Address at peer node (sent by peer broker node to report leader's failure)"
                        + currleadBrokerIPAtNode);
        String currLeadInstanceIPAddressAtServer = coordinatorServerService.getleadEC2BrokerPrivateIP();
        System.out.println("Fetched current lead broker's private IP at coordinator server");
        if (currleadBrokerIPAtNode.equals(currLeadInstanceIPAddressAtServer)) {
            System.out.println(
                    "IP sent by reporting node and stored at Coordinator Server are a match\n verifying if leader broker is indeed non responsive");
            boolean leaderIsAlive = pingLeader(currLeadInstanceIPAddressAtServer);

            if (!leaderIsAlive) {
                System.out.println(
                        "leader node is non responsive. \n Setting value of leader broker at coordinator server as null untill leader election completes.");
                // set leaderInstancePrivateIPAddress to null
                coordinatorServerService.setleadEC2BrokerPrivateIP(null);

                Integer instanceIDOfFailedLeadBroker = findUniqueIdByIpAddress(currLeadInstanceIPAddressAtServer);
                if (instanceIDOfFailedLeadBroker != null) {
                    System.out.println("Deregistering failed leader broker node");
                    deregisterBroker(instanceIDOfFailedLeadBroker);
                    System.out.println("Starting leader election among the other registered broker nodesin cluster");
                    coordinatorServerService.electNewLeaderAndAssociateElasticIp();
                    System.out.println("New leader is elected.");
                    String newLeadBrokerPrivateIPAddress = coordinatorServerService.getleadEC2BrokerPrivateIP();
                    System.out.println("PrivateIP of new leader broker node: " + newLeadBrokerPrivateIPAddress);

                    List<String> peerBrokerIPAddresses = getPeerBrokerIPList();
                    System.out.println(
                            "Sending new elected lead broker's private IP to all registered peer nodes in cluster");
                    sendNewLeadBrokerPrivateIPToPeerNodes(peerBrokerIPAddresses, newLeadBrokerPrivateIPAddress);

                } else {
                    System.out.println(
                            "Failed to find uniqueID associated with the IP address of the failed leader broker node.");
                }
            }

        }
        System.out.println(
                "reporting node does not have updated lead broker node's IP. It didn't match with lead broker's IP stored at Coordinator Server. ");

    }

}
