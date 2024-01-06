package com.practice.springrabbitmqfileprocessing.publisher;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.practice.springrabbitmqfileprocessing.configurations.RabbitMQConfig;
import com.practice.springrabbitmqfileprocessing.domain.InvoiceDetail;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageBuilder;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.stereotype.Component;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Date;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;

@Component
@RequiredArgsConstructor
@Slf4j
public class InvoiceDetailsPublisher {

    private final RabbitTemplate rabbitTemplate;

    private final ObjectMapper objectMapper;

    String value = "";
    public void insertAllInvoiceRecords(File file) {
        try {
            FileReader fileReader = new FileReader(file);
            BufferedReader br = new BufferedReader(fileReader);
            // Get the first row.
            String row[] = br.readLine().split(",");
            // CSV has 2 column with same name as "document_create_date". We will be using
            // the "document_create_date.1".
            row[8] = "document_create_date.1";
            // This map the columnName to its index in row array. This would make easy for
            // us to access the column from csv directly using columnName.
            Map<String, Integer> columnNameToIndexMapper = new HashMap<>();
            for (int i = 0; i < row.length; i++) {
                columnNameToIndexMapper.put(row[i], i);
            }
            // Invoke the insertAllInvoiceRecordsBatch function.
          publishInvoiceDetailsToQueue(br, columnNameToIndexMapper);
        } catch (FileNotFoundException fle) {
            System.out.println("The " + file.getName() + " cannot be read");
        } catch (IOException e) {
            // TODO: handle exception
        }
    }

    private void publishInvoiceDetailsToQueue(BufferedReader br, Map<String, Integer> columnNameToIndexMapper) throws IOException {
        InvoiceDetail invoiceDetail = null;
        String line = null;
        String row[] = null;
        log.info("Publishing records: ");
        while ((line = br.readLine()) != null) {
            // Get the row in the array splitted by ','.
            row = line.split(",");
            // Map the csv row to POJO data field.
            invoiceDetail = getInvoiceDetails(row, columnNameToIndexMapper);
            // If primary key is defined.

            if (invoiceDetail != null) {
                JsonNode invoiceJson = objectMapper.convertValue(invoiceDetail, JsonNode.class);
                byte[] byteArray = invoiceJson.toString().getBytes();
                MessageProperties messageProperties = new MessageProperties();
                messageProperties.setContentType(MessageProperties.CONTENT_TYPE_JSON);
                Message message = MessageBuilder.withBody(byteArray).andProperties(messageProperties).build();
                rabbitTemplate.send(RabbitMQConfig.topicExchangeName, "invoices.created.new", message);
            }
        }
    }

    private InvoiceDetail getInvoiceDetails(String row[], Map<String, Integer> columnNameToIndexMapper) {
        InvoiceDetail invoiceDetail = null;
        String value = "";
        String docId = row[columnNameToIndexMapper.get("doc_id")];
        try {
            // If primary key is null. SKIP
            if (!docId.isEmpty()) {

                invoiceDetail = new InvoiceDetail();
                // Get rid of .0 appended after every docId in my csv.
                docId = docId.substring(0, docId.lastIndexOf('.'));
                invoiceDetail.setDocId(Long.parseLong(docId));

                // For Business Code
                value = row[columnNameToIndexMapper.get("business_code")];

                if (!value.isEmpty())
                    invoiceDetail.setBusinessCode(value);
                else
                    invoiceDetail.setBusinessCode(null);

                // For Customer Name
                value = row[columnNameToIndexMapper.get("name_customer")];
                if (!value.isEmpty())
                    invoiceDetail.setNameCustomer(value);
                else
                    invoiceDetail.setNameCustomer(null);

                // For Customer Number
                value = row[columnNameToIndexMapper.get("cust_number")];
                if (!value.isEmpty())

                    invoiceDetail.setCustNumber(value);
                else
                    invoiceDetail.setCustNumber(null);

                // For Clear Date.
                value = row[columnNameToIndexMapper.get("clear_date")];
                if (!value.isEmpty()) {
                    // Convert String to Timestamp.
                    Timestamp clearDateTimestamp = Timestamp.valueOf(value);
                    invoiceDetail.setClearDate(clearDateTimestamp);
                } else
                    invoiceDetail.setClearDate(null);

                // For Business Year
                value = row[columnNameToIndexMapper.get("buisness_year")];
                if (!value.isEmpty()) {

                    value = value.substring(0, value.lastIndexOf('.'));
                    invoiceDetail.setBusinessYear(Short.parseShort(value));
                } else
                    invoiceDetail.setBusinessYear(null);

                // For Posting Date
                value = row[columnNameToIndexMapper.get("posting_date")];
                if (!value.isEmpty()) {
                    Date postingDate = Date.valueOf(value);
                    invoiceDetail.setPostingDate(postingDate);
                } else
                    invoiceDetail.setPostingDate(null);

                // For Document Create Date
                value = row[columnNameToIndexMapper.get("document_create_date.1")];
                if (!value.isEmpty()) {
                    // Convert the date into yyyy-mm-dd format.
                    Date date = toDateFormat(value);
                    invoiceDetail.setDocumentCreateDate(date);
                } else
                    invoiceDetail.setDocumentCreateDate(null);

                // For Due in Date
                value = row[columnNameToIndexMapper.get("due_in_date")];
                if (!value.isEmpty()) {
                    // Convert the date into yyyy-mm-dddd format.
                    Date date = toDateFormat(value);
                    invoiceDetail.setDueInDate(date);
                } else
                    invoiceDetail.setDueInDate(null);

                // For Invoice Currency
                value = row[columnNameToIndexMapper.get("invoice_currency")];
                if (!value.isEmpty()) {
                    invoiceDetail.setInvoiceCurrency(value);
                } else
                    invoiceDetail.setInvoiceCurrency(null);

                // For Document Type
                value = row[columnNameToIndexMapper.get("document type")];
                if (!value.isEmpty()) {
                    invoiceDetail.setDocumentType(value);
                } else
                    invoiceDetail.setDocumentType(null);

            }

            // For Posting Id
            value = row[columnNameToIndexMapper.get("posting_id")];
            if (!value.isEmpty()) {
                value = value.substring(0, value.lastIndexOf('.'));
                invoiceDetail.setPostingId(Short.parseShort(value));
            } else
                invoiceDetail.setPostingId(null);

            // For Area Business
            value = row[columnNameToIndexMapper.get("area_business")];
            if (!value.isEmpty()) {

                invoiceDetail.setAreaBusiness(value);
            } else
                invoiceDetail.setAreaBusiness(null);

            // For Total Open Amount
            value = row[columnNameToIndexMapper.get("total_open_amount")];
            if (!value.isEmpty()) {

                invoiceDetail.setTotalOpenAmount(Double.parseDouble(value));
            } else
                invoiceDetail.setTotalOpenAmount(null);

            // For Baseline Create Date
            value = row[columnNameToIndexMapper.get("baseline_create_date")];
            if (!value.isEmpty()) {
                // Convert into yyyymmdd format.
                Date date = toDateFormat(value);
                invoiceDetail.setBaselineCreateDate(date);
            } else
                invoiceDetail.setBaselineCreateDate(null);

            // For Customer Payment Terms.
            value = row[columnNameToIndexMapper.get("cust_payment_terms")];
            if (!value.isEmpty()) {

                invoiceDetail.setCustPaymentTerms(value);
            } else
                invoiceDetail.setCustNumber(null);

            // For Invoice - Id
            value = row[columnNameToIndexMapper.get("invoice_id")];
            if (!value.isEmpty()) {
                value = value.substring(0, value.lastIndexOf('.'));
                invoiceDetail.setInvoiceId(Long.parseLong(value));
            } else
                invoiceDetail.setInvoiceId(null);

            // For isOpen
            value = row[columnNameToIndexMapper.get("isOpen")];
            if (!value.isEmpty()) {

                invoiceDetail.setIsOpen(Byte.parseByte(value));
            } else
                invoiceDetail.setIsOpen(null);
        } catch (NullPointerException e) {
            System.out.println(value);
        }
        return invoiceDetail;
    }

    private Date toDateFormat(String dateTime) {
        // Define a SimpleDateFormat with specific pattern.
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");
        java.util.Date convertedDate = null;
        Date sqlDate = null;
        try {
            // Parse date
            convertedDate = dateFormat.parse(dateTime);
            // Set a new pattern
            SimpleDateFormat sdfnewformat = new SimpleDateFormat("yyyy-MM-dd");
            // Convert into that format.
            String finalDateString = sdfnewformat.format(convertedDate);
            sqlDate = Date.valueOf(finalDateString);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return sqlDate;
    }
}
