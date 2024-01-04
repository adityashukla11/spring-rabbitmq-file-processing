package com.practice.springrabbitmqfileprocessing.bootstrap;


import com.practice.springrabbitmqfileprocessing.publisher.InvoiceDetailsPublisher;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import java.io.File;


@Component
@Slf4j
@RequiredArgsConstructor
public class DataProcessor implements CommandLineRunner {
	private final InvoiceDetailsPublisher invoiceDetailsPublisher;
	private final String FILE_NAME = "src/main/resources/csv/initial_data.csv";

	@Override
	public void run(String... args) {
			File csvFile = new File(FILE_NAME);
			invoiceDetailsPublisher.insertAllInvoiceRecords(csvFile);
	}
}
