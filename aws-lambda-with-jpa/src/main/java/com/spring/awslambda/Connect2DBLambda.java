package com.spring.awslambda;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import com.spring.awslambda.models.ConnectSupportLocale;
import com.spring.awslambda.repositories.ConnectSupportLocaleRepository;

import java.util.List;
import java.util.function.Function;

@Component
public class Connect2DBLambda {

	private static final Logger log = LoggerFactory.getLogger(Connect2DBLambda.class);

	private static boolean isColdStart = true;

	@Autowired
	private ConnectSupportLocaleRepository connectSupportLocaleRepository;

	/**
	 * This method acts as lambda function handler.
	 * This takes input as a string, process and returns string. 
	 */
	@Bean
	public Function<String, String> cleanUp() {

		return value -> {

			log.info("DBCleanUpLambda started successfully");
			if (isColdStart) {
				isColdStart = false;
				log.info("Cold start detected!");
			}

			log.info("Going to fetch the data from DB...");
			List<ConnectSupportLocale> supportedLocales = connectSupportLocaleRepository.findAll();

			supportedLocales.forEach(locale -> log.info("localeName:" + locale.getName()));

			log.info("Lambda function executed successfully!");

			return "Lambda function executed successfully!";
		};
	}
}
