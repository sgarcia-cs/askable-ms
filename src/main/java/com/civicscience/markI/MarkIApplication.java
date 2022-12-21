package com.civicscience.markI;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.openfeign.EnableFeignClients;

@SpringBootApplication
@EnableFeignClients
public class MarkIApplication {

	public static void main(String[] args) {
		SpringApplication.run(MarkIApplication.class, args);
	}

}
