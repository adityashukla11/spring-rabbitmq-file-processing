package com.practice.springrabbitmqfileprocessing.domain;

import java.io.Serializable;
import java.sql.Date;
import java.sql.Timestamp;


public class InvoiceDetail implements Serializable {
	
	private String businessCode;
	private String custNumber;
	private String nameCustomer;
	private Timestamp clearDate;
	private Short businessYear;
	private Long docId;
	private Date postingDate;
	private Date documentCreateDate;
	private Date dueInDate;
	private String invoiceCurrency;
	private String documentType;
	private Short postingId;
	private String areaBusiness;
	private Double totalOpenAmount;
	private Date baselineCreateDate;
	private String custPaymentTerms;
	private Long invoiceId;
	private Byte isOpen;
	
	/**
	 * Default Constructor
	 */
	public InvoiceDetail()
	{
		
	}
	
	/**
	 * @param businessCode
	 * @param custNumber
	 * @param nameCustomer
	 * @param clearDate
	 * @param businessYear
	 * @param docId
	 * @param posting_date
	 * @param documentCreateDate
	 * @param dueInDate
	 * @param invoiceCurrency
	 * @param documentType
	 * @param postingId
	 * @param areaBusiness
	 * @param totalOpenAmount
	 * @param baselineCreateDate
	 * @param custPaymentTerms
	 * @param invoiceId
	 * @param isOpen
	 */
	public InvoiceDetail(String businessCode, String custNumber, String nameCustomer, Timestamp clearDate,
                         Short businessYear, Long docId, Date posting_date, Date documentCreateDate, Date dueInDate,
                         String invoiceCurrency, String documentType, Short postingId, String areaBusiness, Double totalOpenAmount,
                         Date baselineCreateDate, String custPaymentTerms, Long invoiceId, Byte isOpen) {
		super();
		this.businessCode = businessCode;
		this.custNumber = custNumber;
		this.nameCustomer = nameCustomer;
		this.clearDate = clearDate;
		this.businessYear = businessYear;
		this.docId = docId;
		this.postingDate = posting_date;
		this.documentCreateDate = documentCreateDate;
		this.dueInDate = dueInDate;
		this.invoiceCurrency = invoiceCurrency;
		this.documentType = documentType;
		this.postingId = postingId;
		this.areaBusiness = areaBusiness;
		this.totalOpenAmount = totalOpenAmount;
		this.baselineCreateDate = baselineCreateDate;
		this.custPaymentTerms = custPaymentTerms;
		this.invoiceId = invoiceId;
		this.isOpen = isOpen;
	}

	/**
	 * @return the businessCode
	 */
	public String getBusinessCode() {
		return businessCode;
	}

	/**
	 * @param businessCode the businessCode to set
	 */
	public void setBusinessCode(String businessCode) {
		this.businessCode = businessCode;
	}

	/**
	 * @return the custNumber
	 */
	public String getCustNumber() {
		return custNumber;
	}

	/**
	 * @param custNumber the custNumber to set
	 */
	public void setCustNumber(String custNumber) {
		this.custNumber = custNumber;
	}

	/**
	 * @return the nameCustomer
	 */
	public String getNameCustomer() {
		return nameCustomer;
	}

	/**
	 * @param nameCustomer the nameCustomer to set
	 */
	public void setNameCustomer(String nameCustomer) {
		this.nameCustomer = nameCustomer;
	}

	/**
	 * @return the clearDate
	 */
	public Timestamp getClearDate() {
		return clearDate;
	}

	/**
	 * @param clearDate the clearDate to set
	 */
	public void setClearDate(Timestamp clearDate) {
		this.clearDate = clearDate;
	}

	/**
	 * @return the businessYear
	 */
	public Short getBusinessYear() {
		return businessYear;
	}

	/**
	 * @param businessYear the businessYear to set
	 */
	public void setBusinessYear(Short businessYear) {
		this.businessYear = businessYear;
	}

	/**
	 * @return the docId
	 */
	public Long getDocId() {
		return docId;
	}

	/**
	 * @param docId the docId to set
	 */
	public void setDocId(Long docId) {
		this.docId = docId;
	}

	/**
	 * @return the posting_date
	 */
	public Date getPostingDate() {
		return postingDate;
	}

	/**
	 * @param posting_date the posting_date to set
	 */
	public void setPostingDate(Date posting_date) {
		this.postingDate = posting_date;
	}

	/**
	 * @return the documentCreateDate
	 */
	public Date getDocumentCreateDate() {
		return documentCreateDate;
	}

	/**
	 * @param documentCreateDate the documentCreateDate to set
	 */
	public void setDocumentCreateDate(Date documentCreateDate) {
		this.documentCreateDate = documentCreateDate;
	}

	/**
	 * @return the dueInDate
	 */
	public Date getDueInDate() {
		return dueInDate;
	}

	/**
	 * @param dueInDate the dueInDate to set
	 */
	public void setDueInDate(Date dueInDate) {
		this.dueInDate = dueInDate;
	}

	/**
	 * @return the invoiceCurrency
	 */
	public String getInvoiceCurrency() {
		return invoiceCurrency;
	}

	/**
	 * @param invoiceCurrency the invoiceCurrency to set
	 */
	public void setInvoiceCurrency(String invoiceCurrency) {
		this.invoiceCurrency = invoiceCurrency;
	}

	/**
	 * @return the documentType
	 */
	public String getDocumentType() {
		return documentType;
	}

	/**
	 * @param documentType the documentType to set
	 */
	public void setDocumentType(String documentType) {
		this.documentType = documentType;
	}

	/**
	 * @return the postingId
	 */
	public Short getPostingId() {
		return postingId;
	}

	/**
	 * @param postingId the postingId to set
	 */
	public void setPostingId(Short postingId) {
		this.postingId = postingId;
	}

	/**
	 * @return the areaBusiness
	 */
	public String getAreaBusiness() {
		return areaBusiness;
	}

	/**
	 * @param areaBusiness the areaBusiness to set
	 */
	public void setAreaBusiness(String areaBusiness) {
		this.areaBusiness = areaBusiness;
	}

	/**
	 * @return the totalOpenAmount
	 */
	public Double getTotalOpenAmount() {
		return totalOpenAmount;
	}

	/**
	 * @param totalOpenAmount the totalOpenAmount to set
	 */
	public void setTotalOpenAmount(Double totalOpenAmount) {
		this.totalOpenAmount = totalOpenAmount;
	}

	/**
	 * @return the baselineCreateDate
	 */
	public Date getBaselineCreateDate() {
		return baselineCreateDate;
	}

	/**
	 * @param baselineCreateDate the baselineCreateDate to set
	 */
	public void setBaselineCreateDate(Date baselineCreateDate) {
		this.baselineCreateDate = baselineCreateDate;
	}

	/**
	 * @return the custPaymentTerms
	 */
	public String getCustPaymentTerms() {
		return custPaymentTerms;
	}

	/**
	 * @param custPaymentTerms the custPaymentTerms to set
	 */
	public void setCustPaymentTerms(String custPaymentTerms) {
		this.custPaymentTerms = custPaymentTerms;
	}

	/**
	 * @return the invoiceId
	 */
	public Long getInvoiceId() {
		return invoiceId;
	}

	/**
	 * @param invoiceId the invoiceId to set
	 */
	public void setInvoiceId(Long invoiceId) {
		this.invoiceId = invoiceId;
	}

	/**
	 * @return the isOpen
	 */
	public Byte getIsOpen() {
		return isOpen;
	}

	/**
	 * @param isOpen the isOpen to set
	 */
	public void setIsOpen(Byte isOpen) {
		this.isOpen = isOpen;
	}

	@Override
	public String toString() {
		return "InvoiceDetail [businessCode=" + businessCode + ", custNumber=" + custNumber + ", nameCustomer="
				+ nameCustomer + ", clearDate=" + clearDate + ", businessYear=" + businessYear + ", docId=" + docId
				+ ", postingDate=" + postingDate + ", documentCreateDate=" + documentCreateDate + ", dueInDate="
				+ dueInDate + ", invoiceCurrency=" + invoiceCurrency + ", documentType=" + documentType + ", postingId="
				+ postingId + ", areaBusiness=" + areaBusiness + ", totalOpenAmount=" + totalOpenAmount
				+ ", baselineCreateDate=" + baselineCreateDate + ", custPaymentTerms=" + custPaymentTerms
				+ ", invoiceId=" + invoiceId + ", isOpen=" + isOpen + "]";
	}
	

}
