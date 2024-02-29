package com.example.CoordinatorServer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.config.server.EnableConfigServer;

@SpringBootApplication
@EnableConfigServer
public class CoordinatorServerApplication {

	public static void main(String[] args) {
		SpringApplication.run(CoordinatorServerApplication.class, args);
	}

}
