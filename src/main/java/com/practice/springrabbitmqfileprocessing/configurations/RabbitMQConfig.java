package com.practice.springrabbitmqfileprocessing.configurations;

import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.support.converter.DefaultClassMapper;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class RabbitMQConfig {
    public static final String topicExchangeName = "invoice-processing";
    private static final String queueName = "created-invoices";

    @Bean
    Queue createdInvoiceQueue() {
        return new Queue(queueName, false);
    }

    @Bean
    TopicExchange topicExchange() {
        return new TopicExchange(topicExchangeName);
    }

    @Bean
    Binding createdInvoiceQueueBinding (Queue queue, TopicExchange topicExchange) {
        return BindingBuilder.bind(queue).to(topicExchange).with("invoices.created.#");
    }

//Not Required with @RabbitListener configurations.
//    @Bean
//    SimpleMessageListenerContainer container(ConnectionFactory connectionFactory,
//                                             MessageListenerAdapter listenerAdapter) {
//        SimpleMessageListenerContainer container = new SimpleMessageListenerContainer();
//        container.setConnectionFactory(connectionFactory);
//        container.setQueueNames(queueName);
//        container.setMessageListener(listenerAdapter);
//        return container;
//    }

//    @Bean
//    public MessageConverter jsonToMapMessageConverter() {
//        DefaultClassMapper defaultClassMapper = new DefaultClassMapper();
////        defaultClassMapper.setTrustedPackages("com.practice.springrabbitmqfileprocessing.domain, com.practice.springrabbitmqinvoiceconsumer.domain"); // trusted packages
//        Jackson2JsonMessageConverter jackson2JsonMessageConverter = new Jackson2JsonMessageConverter();
//        jackson2JsonMessageConverter.setClassMapper(defaultClassMapper);
//        return jackson2JsonMessageConverter;
//    }

//    @Bean
//    MessageListenerAdapter listenerAdapter(InvoiceDetailsReceiver receiver) {
//        MessageListenerAdapter messageListenerAdapter = new MessageListenerAdapter();
//        messageListenerAdapter.setDelegate(receiver);
//        messageListenerAdapter.setDefaultListenerMethod("receivedNewInvoices");
////        messageListenerAdapter.setMessageConverter(messageConverter);
//        return messageListenerAdapter;
//    }

//    @Bean
//    RabbitTemplate rabbitTemplate(final ConnectionFactory connectionFactory, final MessageConverter messageConverter)
//    {
//        final RabbitTemplate rabbitTemplate = new RabbitTemplate();
//        rabbitTemplate.setConnectionFactory(connectionFactory);
//        rabbitTemplate.setMessageConverter(messageConverter);
//        return rabbitTemplate;
//    }

}
