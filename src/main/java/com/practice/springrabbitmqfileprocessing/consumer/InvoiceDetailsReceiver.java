package com.practice.springrabbitmqfileprocessing.consumer;

import com.practice.springrabbitmqfileprocessing.domain.InvoiceDetail;
import org.springframework.amqp.rabbit.annotation.RabbitHandler;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

@Component
@RabbitListener(queues = "created-invoices", messageConverter = "jsonToMapMessageConverter")
public class InvoiceDetailsReceiver {
    @RabbitHandler
    public void receivedNewInvoices(InvoiceDetail invoiceDetail) {
        System.out.println(invoiceDetail);
    }
}
