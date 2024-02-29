package com.example.CoordinatorServer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.bind.annotation.RequestParam;

@RestController
public class BrokerRegistrationController {
    private final CoordinatorServerService coordinatorServerService;

    private final Map<Integer, Broker> brokerRegistry = new HashMap<>();

    @Autowired
    public BrokerRegistrationController(CoordinatorServerService coordinatorServerService) {
        this.coordinatorServerService = coordinatorServerService;
    }

    @GetMapping("/helloConfig")
    public String hello() {
        return "Hello, World! Coordinator service is up & running!";
    }

    @PostMapping("/register-broker")
    public void registerBroker(@RequestBody Broker broker) {
        brokerRegistry.put(broker.getUniqueId(), broker);
        coordinatorServerService.registerInstance(broker.getEC2instanceID(), broker.getUniqueId());

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
        }
    }

    @GetMapping("/fetch-leadBroker-elasticIP")
    public String getLeadBrokerPublicIPAddress() {
        return coordinatorServerService.getElasticIpAddress();
    }

    @GetMapping("/getCurrent-leadBroker-PrivateIP")
    public String getCurrentLeadBrokerPrivateIP(@RequestParam String param) {
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
        return peerBrokerIPList;
    }

}
