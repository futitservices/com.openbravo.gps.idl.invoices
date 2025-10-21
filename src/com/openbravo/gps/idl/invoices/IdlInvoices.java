/*
 * *************************************************************************
 * Copyright (C) 2011 Openbravo S.L.U.
 *
 * Licensed under the Openbravo Commercial License version 1.0
 * You may obtain a copy of the License at http://www.openbravo.com/legal/obcl.html
 *
 *************************************************************************
 */

package com.openbravo.gps.idl.invoices;

import java.io.FileReader;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.Vector;

import javax.servlet.ServletException;

import org.hibernate.Query;
import org.hibernate.criterion.Expression;
import org.openbravo.base.exception.OBException;
import org.openbravo.base.provider.OBProvider;
import org.openbravo.base.session.OBPropertiesProvider;
import org.openbravo.base.structure.BaseOBObject;
import org.openbravo.dal.core.OBContext;
import org.openbravo.dal.service.OBCriteria;
import org.openbravo.dal.service.OBDal;
import org.openbravo.data.UtilSql;
import org.openbravo.erpCommon.reference.PInstanceProcessData;
import org.openbravo.erpCommon.utility.OBError;
import org.openbravo.erpCommon.utility.Utility;
import org.openbravo.idl.proc.Parameter;
import org.openbravo.idl.proc.Validator;
import org.openbravo.idl.proc.Value;
import org.openbravo.model.ad.access.User;
import org.openbravo.model.ad.process.ProcessInstance;
import org.openbravo.model.ad.ui.Process;
import org.openbravo.model.common.businesspartner.BusinessPartner;
import org.openbravo.model.common.enterprise.DocumentType;
import org.openbravo.model.common.enterprise.Organization;
import org.openbravo.model.common.invoice.Invoice;
import org.openbravo.model.common.invoice.InvoiceLine;
import org.openbravo.model.common.plm.Product;
import org.openbravo.model.common.uom.UOM;
import org.openbravo.model.financialmgmt.gl.GLItem;
import org.openbravo.model.financialmgmt.payment.FIN_PaymentMethod;
import org.openbravo.model.financialmgmt.payment.PaymentTerm;
import org.openbravo.model.financialmgmt.tax.TaxRate;
import org.openbravo.model.financialmgmt.tax.Withholding;
import org.openbravo.model.pricing.pricelist.PriceList;
import org.openbravo.model.pricing.pricelist.PriceListVersion;
import org.openbravo.model.pricing.pricelist.ProductPrice;
import org.openbravo.model.project.Project;
import org.openbravo.module.idljava.proc.IdlServiceJava;

import au.com.bytecode.opencsv.CSVReader;

import org.openbravo.model.financialmgmt.accounting.UserDimension1;
import org.openbravo.model.financialmgmt.accounting.UserDimension2;
import org.hibernate.criterion.Restrictions;

public class IdlInvoices extends IdlServiceJava {

  private Invoice PreviousInvoice;
  private int TotalLines;
  private int ActualLine;

  @Override
  protected boolean executeImport(String filename, boolean insert) throws Exception {

    CSVReader reader = new CSVReader(new FileReader(filename), ',', '\"', '\\', 0, false, true);

    List AllLinesList = reader.readAll();
    TotalLines = AllLinesList.size();
    PreviousInvoice = null;

    String[] nextLine;

    // Check header
    nextLine = (String[]) AllLinesList.get(0);
    if (nextLine == null) {
      throw new OBException(Utility.messageBD(conn, "IDLJAVA_HEADER_MISSING", vars.getLanguage()));
    }
    Parameter[] parameters = getParameters();
    if (parameters.length > nextLine.length) {
      throw new OBException(Utility
          .messageBD(conn, "IDLJAVA_HEADER_BAD_LENGTH", vars.getLanguage()));
    }

    Validator validator;

    for (int i = 1; i < TotalLines; i++) {

      nextLine = (String[]) AllLinesList.get(i);
      ActualLine = i;

      if (nextLine.length > 1 || nextLine[0].length() > 0) {
        // It is not an empty line

        // Validate types
        if (parameters.length > nextLine.length) {
          throw new OBException(Utility.messageBD(conn, "IDLJAVA_LINE_BAD_LENGTH", vars
              .getLanguage()));
        }

        validator = getValidator(getEntityName());
        Object[] result = validateProcess(validator, nextLine);
        if ("0".equals(validator.getErrorCode())) {
          finishRecordProcess(result);
        } else {
          OBDal.getInstance().rollbackAndClose();
          // We need rollback here becouse the intention is load ALL or NOTHING
          logRecordError(validator.getErrorMessage(), result);
        }
      }
    }

    return true;
  }

  @Override
  public String getEntityName() {
    return "Invoices";
  }

  @Override
  public Parameter[] getParameters() {
    return new Parameter[] { new Parameter("IsSalesOrderTransaction", Parameter.STRING),
        new Parameter("Organization", Parameter.STRING),
        new Parameter("DocumentNo", Parameter.STRING),
        new Parameter("OrderReference", Parameter.STRING),
        new Parameter("DescriptionHeader", Parameter.STRING),
        new Parameter("TransactionDocument", Parameter.STRING),
        new Parameter("InvoiceDate", Parameter.STRING),
        new Parameter("AccountingDate", Parameter.STRING),
        new Parameter("TaxDate", Parameter.STRING),
        new Parameter("BusinessPartner", Parameter.STRING),
        new Parameter("UserContact", Parameter.STRING),
        new Parameter("PriceList", Parameter.STRING),
        new Parameter("SalesRepresentativeCompanyAgent", Parameter.STRING),
        new Parameter("PrintDiscount", Parameter.STRING),
        new Parameter("FormOfPayment", Parameter.STRING),
        new Parameter("PaymentsTerms", Parameter.STRING),
        new Parameter("Project", Parameter.STRING),
        new Parameter("FinancialInvoiceLine", Parameter.STRING),
        new Parameter("Account", Parameter.STRING), new Parameter("Product", Parameter.STRING),
        new Parameter("DescriptionLine", Parameter.STRING),
        new Parameter("InvoicedQuantity", Parameter.STRING),
        new Parameter("PriceListVersion", Parameter.STRING),
        new Parameter("NetUnitPrice", Parameter.STRING),
        new Parameter("NetListPrice", Parameter.STRING), new Parameter("Tax", Parameter.STRING),
        new Parameter("Process", Parameter.STRING), new Parameter("Withholding", Parameter.STRING),
        new Parameter("ExcludeForWithholding", Parameter.STRING),
        new Parameter("User1", Parameter.STRING),
        new Parameter("User2", Parameter.STRING)};
  }

  @Override
  protected Object[] validateProcess(Validator validator, String... values) throws Exception {
    validator.checkNotNull(validator.checkBoolean(values[0], "IsSalesOrderTransaction"),
        "IsSalesOrderTransaction");
    validator.checkNotNull(validator.checkString(values[1], 40, "Organization"), "Organization");
    validator.checkOrganization(values[1]);
    validator.checkTransactionalOrganization(values[1]);
    validator.checkString(values[2], 30, "DocumentNo");
    validator.checkString(values[3], 20, "OrderReference");
    validator.checkString(values[4], 255, "DescriptionHeader");
    validator.checkNotNull(validator.checkString(values[5], 60, "TransactionDocument"),
        "TransactionDocument");
    validator.checkNotNull(validator.checkDate(values[6], "InvoiceDate"), "InvoiceDate");
    validator.checkDate(values[7], "AccountingDate");
    validator.checkDate(values[8], "TaxDate");
    validator.checkNotNull(validator.checkString(values[9], 40, "BusinessPartner"),
        "BusinessPartner");
    validator.checkString(values[10], 40, "UserContact");
    validator.checkString(values[11], 60, "PriceList");
    validator.checkString(values[12], 40, "SalesRepresentativeCompanyAgent");
    validator.checkBoolean(values[13], "PrintDiscount");
    validator.checkString(values[14], 32, "FormOfPayment");
    validator.checkString(values[15], 60, "PaymentsTerms");
    validator.checkString(values[16], 40, "Project");
    validator.checkBoolean(values[17], "FinancialInvoiceLine");
    validator.checkString(values[18], 60, "Account");
    validator.checkString(values[19], 40, "Product");
    validator.checkString(values[20], 255, "DescriptionLine");
    validator.checkNotNull(validator.checkBigDecimal(values[21], "InvoicedQuantity"),
        "InvoicedQuantity");
    validator.checkString(values[22], 60, "PriceListVersion");
    validator.checkBigDecimal(values[23], "NetUnitPrice");
    validator.checkBigDecimal(values[24], "NetListPrice");
    validator.checkString(values[25], 60, "Tax");
    validator.checkNotNull(validator.checkBoolean(values[26], "Process"), "Process");
    validator.checkString(values[27], 60, "Withholding");
    validator.checkBoolean(values[28], "ExcludeForWithholding");

    // After Validate go to catch Default Value if exits
    values = getDefaultValues(values);

    return values;
  }

  @Override
  public BaseOBObject internalProcess(Object... values) throws Exception {
    return createInvoice((String) values[0], (String) values[1], (String) values[2],
        (String) values[3], (String) values[4], (String) values[5], (String) values[6],
        (String) values[7], (String) values[8], (String) values[9], (String) values[10],
        (String) values[11], (String) values[12], (String) values[13], (String) values[14],
        (String) values[15], (String) values[16], (String) values[17], (String) values[18],
        (String) values[19], (String) values[20], (String) values[21], (String) values[22],
        (String) values[23], (String) values[24], (String) values[25], (String) values[26],
        (String) values[27], (String) values[28], (String) values[29], (String) values[30]);
  }
  public UserDimension1 getUser1 (String strUser1) {

    OBCriteria<UserDimension1> ac = OBDal.getInstance().createCriteria(UserDimension1.class);

    ac.add(Restrictions.eq(UserDimension1.PROPERTY_SEARCHKEY,strUser1));

    ac.setMaxResults(1);

    return (UserDimension1)ac.uniqueResult();

  }

  public UserDimension2 getUser2 (String strUser2) {

    OBCriteria<UserDimension2> ac = OBDal.getInstance().createCriteria(UserDimension2.class);

    ac.add(Restrictions.eq(UserDimension2.PROPERTY_SEARCHKEY,strUser2));

    ac.setMaxResults(1);

    return (UserDimension2)ac.uniqueResult();

  }

  public BaseOBObject createInvoice(final String strIsSalesOrderTransaction,
      final String strOrganization, final String strDocumentNo, final String strOrderReference,
      final String strDescriptionHeader, final String strTransactionDocument,
      final String strInvoiceDate, final String strAccountingDate, final String strTaxDate,
      final String strBusinessPartner, final String strUserContact, final String strPriceList,
      final String strSalesRepresentativeCompanyAgent, final String strPrintDiscount,
      final String strFormOfPayment, final String strPaymentsTerms, final String strProject,
      final String strFinancialInvoiceLine, final String strAccount, final String strProduct,
      final String strDescriptionLine, final String strInvoicedQuantity,
      final String strPriceListVersion, final String strNetUnitPrice, final String strNetListPrice,
      final String strTax, final String strProcess, final String strWithholding,
      final String strExcludeForWithholding, final String strUser1, final String strUser2) throws Exception {

    // Organization -- mandatory
    OBCriteria OrganizationCriteria = OBDal.getInstance().createCriteria(Organization.class);
    OrganizationCriteria.add(Expression.eq("searchKey", strOrganization));
    List<Organization> OrgList = OrganizationCriteria.list();
    Organization Orginvoice = null;
    if (OrgList.isEmpty()) {
      throw new OBException(Utility.messageBD(conn, "IDLEI_ORG_NOT_FOUND", vars.getLanguage())
          + strOrganization);
    } else {
      Orginvoice = OrgList.get(0);
    }

    Boolean issotrx = Parameter.BOOLEAN.parse(strIsSalesOrderTransaction);

    // Invoice -- check if invoice exists and if it has been loaded in other Load
    //
    Invoice invEx = null;
    String uniqueId = "";

    OBCriteria InvoiceCriteria = OBDal.getInstance().createCriteria(Invoice.class);
    if (strDocumentNo.equals("AUTO")) {
      // If DocumentNo is AUTO it will be a New Invoice and orderReference may exists
      // Only check if is a line from the previous invoice
      InvoiceCriteria.add(Expression.eq("orderReference", strOrderReference));
      InvoiceCriteria.add(Expression.eq("documentNo", (PreviousInvoice == null ? ""
          : PreviousInvoice.getDocumentNo())));
      uniqueId = "Order Reference :" + strOrderReference;
    } else if (!strDocumentNo.equals("")) {
      InvoiceCriteria.add(Expression.eq("documentNo", strDocumentNo));
      uniqueId = "DocumentNo :" + strDocumentNo;
    } else if (!strOrderReference.equals("")) {
      InvoiceCriteria.add(Expression.eq("orderReference", strOrderReference));
      uniqueId = "Order Reference :" + strOrderReference;
    } else {
      // Either Document No or Order Reference must exists
      throw new OBException(Utility.messageBD(conn, "IDLEI_NOTDOCUMENTNO_NOTORDERREFERENCE", vars
          .getLanguage()));
    }
    InvoiceCriteria.add(Expression.eq("organization", Orginvoice));
    InvoiceCriteria.add(Expression.eq("salesTransaction", issotrx));
    InvoiceCriteria.add(Expression.isNotNull("iDLEIPROCESS"));
    List<Invoice> InvoiceList = InvoiceCriteria.list();
    if (!InvoiceList.isEmpty()) {
      // Invoice Exists now check if Invoice had been Imported Before
      invEx = InvoiceList.get(0);
      if (invEx.getIDLEIPROCESS().equals("A") || invEx.getIDLEIPROCESS().equals("L")) {
        // Status "A" means had been loaded and processed (all) and status "L" means had been loaded
        throw new OBException(Utility.messageBD(conn, "IDLEI_IMPORTED_BEFORE", vars.getLanguage())
            + uniqueId);
      }
    }

    if (invEx == null) {
      // Before Create a New Invoice process the previous one saved in PreviousInvoice
      if (ActualLine != 1)
        ProcessInvoice(PreviousInvoice, true);

      // Create New Invoice
      Invoice inv = OBProvider.getInstance().get(Invoice.class);
      inv.setSalesTransaction(issotrx);
      inv.setOrganization(Orginvoice);
      Boolean Process = Parameter.BOOLEAN.parse(strProcess);
      // Status "Y" means it will be processed status "N" means it wont
      inv.setIDLEIPROCESS(Process ? "Y" : "N");

      // Document Type -- mandatory
      DocumentType docTypeInv = findDALInstance(false, DocumentType.class, new Value("name",
          strTransactionDocument));
      if (docTypeInv == null) {
        throw new OBException(Utility
            .messageBD(conn, "IDLEI_DOCTYPE_NOT_FOUND", vars.getLanguage())
            + strTransactionDocument);
      }
      inv.setDocumentType(OBDal.getInstance().get(DocumentType.class, "0"));
      inv.setTransactionDocument(docTypeInv);

      // DocumentoNo

      String DocumentNoInv = strDocumentNo;
      if (DocumentNoInv.equals("") || DocumentNoInv.equals("AUTO")) {
        if (docTypeInv.isSequencedDocument()) {
          DocumentNoInv = Utility.getDocumentNo(conn, vars, "", "C_Invoice", "",
              docTypeInv.getId(), true, true);
        } else {
          DocumentNoInv = Utility.getDocumentNo(conn, vars.getClient(), "C_Invoice", false);
        }
      }
      inv.setDocumentNo(DocumentNoInv);

      // Description Header
      inv.setDescription(strDescriptionHeader);

      // SalesRep
      User salesRep = null;
      if (!strSalesRepresentativeCompanyAgent.equals("")) {
        salesRep = findDALInstance(false, User.class, new Value("name",
            strSalesRepresentativeCompanyAgent));
        if (salesRep == null) {
          throw new OBException(Utility.messageBD(conn, "IDLEI_SALESREP_NOT_FOUND", vars
              .getLanguage())
              + strSalesRepresentativeCompanyAgent);
        }
      } else {
        salesRep = findDALInstance(false, User.class, new Value("businessPartner", findDALInstance(
            false, BusinessPartner.class, new Value("isSalesRepresentative", true))));
      }
      inv.setSalesRepresentative(salesRep);

      // Invoice Date -- mandatory
      inv.setInvoiceDate(Parameter.DATE.parse(strInvoiceDate));

      // AccDate
      String AccDateInv = strAccountingDate;
      if (AccDateInv.equals(""))
        AccDateInv = strInvoiceDate;
      inv.setAccountingDate(Parameter.DATE.parse(AccDateInv));

      // Business Partner -- mandatory
      BusinessPartner bpInvoice = findDALInstance(false, BusinessPartner.class, new Value(
          "searchKey", strBusinessPartner));
      if (bpInvoice == null) {
        throw new OBException(Utility.messageBD(conn, "IDLEI_BPARTNER_NOT_FOUND", vars
            .getLanguage())
            + strBusinessPartner);
      }
      inv.setBusinessPartner(bpInvoice);

      // PartnerAddress
      inv.setPartnerAddress(findDALInstance(false,
          org.openbravo.model.common.businesspartner.Location.class, new Value("businessPartner",
              bpInvoice), new Value("invoiceToAddress", true)));

      // OrderReference
      inv.setOrderReference(strOrderReference);

      // PrintDiscount
      inv.setPrintDiscount(bpInvoice.isPrintDiscount());

      // PriceList -- mandatory -- default from partner
      PriceList plistInvoice = null;
      if (!strPriceList.equals("")) {
        // Find PriceList from the file
        plistInvoice = findDALInstance(false, PriceList.class, new Value("name", strPriceList));
        if (plistInvoice == null) {
          throw new OBException(Utility.messageBD(conn, "IDLEI_PRICELIST_NOT_FOUND", vars
              .getLanguage())
              + strPriceList);
        }
      } else {
        // Default PriceList from BPartner
        if (issotrx) {
          plistInvoice = bpInvoice.getPriceList();
        } else {
          plistInvoice = bpInvoice.getPurchasePricelist();
        }
      }
      if (plistInvoice == null) {
        throw new OBException(Utility.messageBD(conn, "IDLEI_PRICELIST_NOT_FOUND", vars
            .getLanguage())
            + Utility.messageBD(conn, "IDLEI_NOT_DEFAULT_VALUE", vars.getLanguage()));
      }
      inv.setPriceList(plistInvoice);

      // Currency
      inv.setCurrency(plistInvoice.getCurrency());

      // IcludesTax
      inv.setPriceIncludesTax(plistInvoice.isPriceIncludesTax());

      // PayRule -- mandatory -- default from partner
      FIN_PaymentMethod paymentmethod = null;
      if (!strFormOfPayment.equals("")) {
        // Find PayRule from the file
        OBCriteria PaymentMethodCriteria = OBDal.getInstance().createCriteria(
            FIN_PaymentMethod.class);
        PaymentMethodCriteria.add(Expression.eq("name", strFormOfPayment));
        List<FIN_PaymentMethod> PaymentMethodList = PaymentMethodCriteria.list();
        if (PaymentMethodList.isEmpty()) {
          throw new OBException(Utility.messageBD(conn, "IDLEI_FORMPAYMENT_NOT_FOUND", vars
              .getLanguage())
              + strFormOfPayment);
        } else {
          paymentmethod = PaymentMethodList.get(0);
        }
      } else {
        // Default PayRule from BPartner
        if (issotrx) {
          paymentmethod = bpInvoice.getPaymentMethod();
        } else {
          paymentmethod = bpInvoice.getPOPaymentMethod();
        }
      }

      if (paymentmethod.equals("")) {// TODO: si es nulo peta!
        throw new OBException(Utility.messageBD(conn, "IDLEI_FORMPAYMENT_NOT_FOUND", vars
            .getLanguage())
            + Utility.messageBD(conn, "IDLEI_NOT_DEFAULT_VALUE", vars.getLanguage()));
      }
      inv.setPaymentMethod(paymentmethod);

      // PayTerm -- mandatory -- default from partner
      PaymentTerm payTermInvoice = null;
      if (!strPaymentsTerms.equals("")) {
        // Find PayTerm from the file
        payTermInvoice = findDALInstance(false, PaymentTerm.class, new Value("searchKey",
            strPaymentsTerms));
        if (payTermInvoice == null) {
          throw new OBException(Utility.messageBD(conn, "IDLEI_PAYMENTTERM_NOT_FOUND", vars
              .getLanguage())
              + strPaymentsTerms);
        }
      } else {
        // Default PayTerm from BPartner
        if (issotrx) {
          payTermInvoice = bpInvoice.getPaymentTerms();
        } else {
          payTermInvoice = bpInvoice.getPOPaymentTerms();
        }
      }
      if (payTermInvoice == null) {
        throw new OBException(Utility.messageBD(conn, "IDLEI_PAYMENTTERM_NOT_FOUND", vars
            .getLanguage())
            + Utility.messageBD(conn, "IDLEI_NOT_DEFAULT_VALUE", vars.getLanguage()));
      }
      inv.setPaymentTerms(payTermInvoice);

      // Project
      Project project = null;
      if (!strProject.equals("")) {
        project = findDALInstance(false, Project.class, new Value("searchKey", strProject));
        if (project == null) {
          throw new OBException(Utility.messageBD(conn, "IDLEI_PROJECT_NOT_FOUND", vars
              .getLanguage())
              + strProject);
        }
        inv.setProject(project);
      }

      // UserConcat
      User UserConcat = null;
      if (!strUserContact.equals("")) {
        UserConcat = findDALInstance(false, User.class, new Value("name", strUserContact));
        if (UserConcat == null) {
          throw new OBException(Utility.messageBD(conn, "IDLEI_USERCONTACT_NOT_FOUND", vars
              .getLanguage())
              + strUserContact);
        }
      } else {
        UserConcat = findDALInstance(false, User.class, new Value("businessPartner", bpInvoice));
      }
      inv.setUserContact(UserConcat);

      // TaxDate
      String TaxDate = strTaxDate;
      if (TaxDate.equals(""))
        TaxDate = strInvoiceDate;
      inv.setTaxDate(Parameter.DATE.parse(TaxDate));

      // WithHolding
      if (!issotrx) {
        Withholding wholdingInvoice = null;
        if (!strWithholding.equals("")) {
          wholdingInvoice = findDALInstance(false, Withholding.class, new Value("name",
              strWithholding));
          if (wholdingInvoice == null) {
            throw new OBException(Utility.messageBD(conn, "IDLEI_WHOLDING_NOT_FOUND", vars
                .getLanguage())
                + strWithholding);
          }
          inv.setWithholding(wholdingInvoice);
        }
      }

      inv.setSummedLineAmount(BigDecimal.ZERO);
      inv.setGrandTotalAmount(BigDecimal.ZERO);

      OBDal.getInstance().save(inv);
      OBDal.getInstance().flush();

      PreviousInvoice = inv;

      invEx = inv;
    }

    // AddLine

    InvoiceLine invLine = OBProvider.getInstance().get(InvoiceLine.class);
    invLine.setInvoice(invEx);
    invLine.setOrganization(invEx.getOrganization());
    // LineNo
    invLine.setLineNo(getInvoiceLineNum(invEx.getId()));
    // Description Line
    invLine.setDescription(strDescriptionLine);

    Boolean isFinInvLine = Parameter.BOOLEAN.parse(strFinancialInvoiceLine);
    invLine.setFinancialInvoiceLine(isFinInvLine);

    Product product = null;

    // Prices
    BigDecimal UnitPrice = Parameter.BIGDECIMAL.parse(strNetUnitPrice);
    BigDecimal ListPrice = Parameter.BIGDECIMAL.parse(strNetListPrice);

    if (isFinInvLine && !strAccount.equals("")) {
      // Account Line
      GLItem account = findDALInstanceOrderByOrg(false, GLItem.class,
          new Value("name", strAccount), new Value("enableInFinancialInvoices", true));
      if (account == null) {
        throw new OBException(
            Utility.messageBD(conn, "IDLEI_ACCOUNT_NOT_FOUND", vars.getLanguage()) + strAccount);
      }
      invLine.setAccount(account);
      // Pricing Accounting
      invLine.setUnitPrice(UnitPrice == null ? BigDecimal.ZERO : UnitPrice);
      invLine.setListPrice(ListPrice == null ? BigDecimal.ZERO : ListPrice);
      invLine.setPriceLimit(ListPrice == null ? BigDecimal.ZERO : ListPrice);
    } else if (!isFinInvLine && !strProduct.equals("")) {

      // Product
      product = findDALInstance(false, Product.class, new Value("searchKey", strProduct));
      if (product == null) {
        throw new OBException(Utility
            .messageBD(conn, "IDLEI_PRODUCT_NOT_FOUND", vars.getLanguage())
            + strProduct);
      }
      invLine.setProduct(product);

      // Pricing Product
      PriceListVersion plistversion = null;
      ProductPrice productprice = null;

      if (!strPriceListVersion.equals("")) {
        // PriceListversion from the file
        plistversion = findDALInstance(false, PriceListVersion.class, new Value("name",
            strPriceListVersion));
        if (plistversion == null) {
          throw new OBException(Utility.messageBD(conn, "IDLEI_PRICELISTVERSION_NOTFOUND", vars
              .getLanguage())
              + strPriceListVersion);
        }

      } else {
        // PriceListVersion from the PriceList
        plistversion = getPriceListVersion(invEx.getPriceList(), invEx.getInvoiceDate());
      }

      if (plistversion != null) {
        productprice = findDALInstance(false, ProductPrice.class, new Value("priceListVersion",
            plistversion), new Value("product", product));
      }

      if (UnitPrice == null && productprice == null) {
        // Price of product not found
        throw new OBException(Utility.messageBD(conn, "IDLEI_NOTPRICE_NOTPRICELIST", vars
            .getLanguage())
            + strProduct);
      }

      // UnitPrice
      if (UnitPrice == null) {
        if (strPriceListVersion.equals("")) {
          // If user dont fill pricelistVersion use getOffersPriceInvoice
          UnitPrice = getOffersPriceInvoice(strInvoiceDate, invEx.getBusinessPartner().getId(),
              product.getId(), productprice.getStandardPrice().toString(), strInvoicedQuantity,
              invEx.getPriceList().getId(), invEx.getId());
          if (UnitPrice == null)
            UnitPrice = productprice.getStandardPrice();
        } else {
          // If user indicate pricelistVersion use PriceListVersion
          UnitPrice = productprice.getStandardPrice();
        }
      }
      invLine.setUnitPrice(UnitPrice);

      // List Price
      if (ListPrice == null && productprice == null) {
        ListPrice = UnitPrice;
      } else if (ListPrice == null) {
        ListPrice = productprice.getListPrice();
      }
      invLine.setListPrice(ListPrice);

      // PriceStd
      if (productprice == null) {
        invLine.setStandardPrice(UnitPrice);
      } else {
        invLine.setStandardPrice(productprice.getStandardPrice());
      }

      // PriceLimit
      if (productprice == null) {
        invLine.setPriceLimit(ListPrice);
      } else {
        invLine.setPriceLimit(productprice.getPriceLimit());
      }

    } else {
      // Empty Line
      invLine.setUnitPrice(UnitPrice == null ? BigDecimal.ZERO : UnitPrice);
      invLine.setListPrice(ListPrice == null ? BigDecimal.ZERO : ListPrice);
      invLine.setPriceLimit(ListPrice == null ? BigDecimal.ZERO : ListPrice);
    }

    // Qty
    BigDecimal Qty = Parameter.BIGDECIMAL.parse(strInvoicedQuantity);
    invLine.setInvoicedQuantity(Qty);

    // LineNetAmt
    BigDecimal LineNetAmt = null;
    if (UnitPrice == null || UnitPrice.equals(BigDecimal.ZERO) || Qty == null
        || Qty.equals(BigDecimal.ZERO)) {
      LineNetAmt = BigDecimal.ZERO;
    } else {
      LineNetAmt = Qty.multiply(UnitPrice);
    }
    invLine.setLineNetAmount(LineNetAmt);

    // Uom
    if (product != null) {
      invLine.setUOM(product.getUOM());
    } else {
      UOM uom = findDALInstance(false, UOM.class, new Value("default", true));
      if (uom == null)
        throw new OBException(Utility.messageBD(conn, "IDLEI_DEFAULT_UOM_NOT_FOUND", vars
            .getLanguage())
            + strTax);
      invLine.setUOM(uom);
    }

    // Tax
    TaxRate tax = null;
    if (!strTax.equals("")) {
      tax = findDALInstanceOrderByOrg(false, TaxRate.class, new Value("name", strTax));
      if (tax == null) {
        throw new OBException(Utility.messageBD(conn, "IDLEI_TAX_NOT_FOUND", vars.getLanguage())
            + strTax);
      }
      invLine.setTax(tax);
    }

    // TaxAmt
    if (tax != null) {
      BigDecimal taxRate = BigDecimal.ZERO;
      Integer taxScale = Integer.valueOf(0);
      taxRate = (tax.getRate() == null ? new BigDecimal(1) : tax.getRate());
      taxScale = Integer.valueOf(invEx.getCurrency().getPricePrecision().toString());
      BigDecimal taxAmt = ((LineNetAmt.multiply(taxRate)).divide(new BigDecimal("100"), 12,
          BigDecimal.ROUND_HALF_EVEN)).setScale(taxScale, BigDecimal.ROUND_HALF_UP);
    }
    
    // ExcludeForWithHolding
    if (!issotrx)
      invLine.setExcludeforwithholding(Parameter.BOOLEAN
          .parse((strExcludeForWithholding.equals("") ? "FALSE" : strExcludeForWithholding)));

    // TaxableAmt
    invLine.setTaxableAmount(LineNetAmt);
    invLine.setStDimension(getUser1(strUser1));
    invLine.setNdDimension(getUser2(strUser2));

    OBDal.getInstance().save(invLine);
    OBDal.getInstance().flush();

    // If is the last line process the invoice (next line can't process it because is the last)
    if (ActualLine == TotalLines - 1) {
      ProcessInvoice(PreviousInvoice, false);
    }

    return invEx;

  }

  public static Long getInvoiceLineNum(String srcCInvoiceId) {
    String hql = "  SELECT COALESCE(MAX(cl.lineNo),0)+10 AS DefaultValue FROM InvoiceLine cl WHERE cl.invoice.id= '"
        + srcCInvoiceId + "'";
    Query q = OBDal.getInstance().getSession().createQuery(hql);

    if (q.list().size() > 0) {
      return Long.valueOf(q.list().get(0).toString());
    } else {
      return 0L;
    }
  }

  public BigDecimal getOffersPriceInvoice(String strdateordered, String strcBpartnerId,
      String strmProductId, String strpricestd, String strqty, String strpricelist,
      String strinvoiceid) throws ServletException {

    String strReturn = "";
    // call the SP
    try {
      ResultSet result = null;
      // first get a connection
      final Connection connection = OBDal.getInstance().getConnection();
      // connection.createStatement().execute("CALL M_InOut_Create0(?)");
      PreparedStatement ps = null;

      String strSql = " SELECT ROUND(M_GET_OFFERS_PRICE(TO_DATE('"
          + strdateordered
          + "'),'"
          + strcBpartnerId
          + "','"
          + strmProductId
          + "',TO_NUMBER('"
          + strpricestd
          + "'), TO_NUMBER('"
          + strqty
          + "'), '"
          + strpricelist
          + "'),(SELECT PRICEPRECISION FROM C_CURRENCY C,C_INVOICE  I WHERE C.C_CURRENCY_ID = I.C_CURRENCY_ID AND I.C_INVOICE_ID  = '"
          + strinvoiceid + "')) AS TOTAL FROM DUAL";

      ps = connection.prepareStatement(strSql);

      result = ps.executeQuery();
      if (result.next()) {
        strReturn = UtilSql.getValue(result, "total");
      }
      result.close();

    } catch (Exception e) {
      throw new IllegalStateException(e);
    }

    if (strReturn.equals("")) {
      return null;
    } else {
      return new BigDecimal(strReturn);
    }

  }

  protected void ProcessInvoice(Invoice invoice, boolean prev) throws Exception {

    if (invoice.getIDLEIPROCESS().equals("Y")) {
      // To be Processed
      completeInvoice(invoice, prev);
      invoice.setIDLEIPROCESS("A");
    } else if (invoice.getIDLEIPROCESS().equals("N")) {
      // Not to be Processed
      invoice.setIDLEIPROCESS("L");
    } else {
      throw new OBException(Utility.messageBD(conn, "IDLEI_INVOICE_BAD_STATUS", vars.getLanguage()));
    }
    OBDal.getInstance().save(invoice);
    OBDal.getInstance().flush();

  }

  protected void completeInvoice(Invoice invoice, boolean prev) throws Exception {

    OBContext.setAdminMode();

    // get the process, we know that 199 is the generate shipments from invoice sp
    final Process process = OBDal.getInstance().get(Process.class, "111");

    // Create the pInstance
    final ProcessInstance pInstance = OBProvider.getInstance().get(ProcessInstance.class);
    // sets its process
    pInstance.setProcess(process);
    // must be set to true
    pInstance.setActive(true);
    pInstance.setRecordID(invoice.getId());
    // get the user from the context
    pInstance.setUserContact(OBContext.getOBContext().getUser());

    // // now create a parameter and set its values
    // final Parameter parameter = OBProvider.getInstance().get(Parameter.class);
    // parameter.setSequenceNumber("1");
    // parameter.setParameterName("Selection");
    // parameter.setString("Y");
    //
    // // set both sides of the bidirectional association
    // pInstance.getADParameterList().add(parameter);
    // parameter.setProcessInstance(pInstance);

    // persist to the db
    OBDal.getInstance().save(pInstance);

    // flush, this gives pInstance an ID
    OBDal.getInstance().flush();

    // call the SP
    try {
      // first get a connection
      final Connection connection = OBDal.getInstance().getConnection();
      // connection.createStatement().execute("CALL M_InOut_Create0(?)");
      PreparedStatement ps = null;
      final Properties obProps = OBPropertiesProvider.getInstance().getOpenbravoProperties();
      if (obProps.getProperty("bbdd.rdbms") != null
          && obProps.getProperty("bbdd.rdbms").equals("POSTGRE")) {
        ps = connection.prepareStatement("SELECT * FROM c_invoice_post0(?)");
      } else {
        ps = connection.prepareStatement("CALL c_invoice_post0(?)");
      }
      ps.setString(1, pInstance.getId());
      ps.execute();

    } catch (Exception e) {
      throw new IllegalStateException(e);
    }

    // refresh the pInstance as the SP has changed it
    OBDal.getInstance().getSession().refresh(pInstance);

    if (pInstance.getResult() == 0) {
      // Error Processing
      OBError myMessage = Utility
          .getProcessInstanceMessage(conn, vars, getPInstanceData(pInstance));
      throw new OBException((prev ? Utility.messageBD(conn, "IDLEI_PREVIOUS_INVOICE", vars
          .getLanguage()) : Utility.messageBD(conn, "IDLEI_INVOICE", vars.getLanguage()))
          + invoice.getDocumentNo()
          + " / "
          + invoice.getOrderReference()
          + Utility.messageBD(conn, "IDLEI_PROCESS_ERROR_STATUS", vars.getLanguage())
          + " ERROR: "
          + myMessage.getMessage());
    }

    OBContext.restorePreviousMode();

  }

  protected PInstanceProcessData[] getPInstanceData(ProcessInstance pInstance) throws Exception {
    Vector<java.lang.Object> vector = new Vector<java.lang.Object>(0);
    PInstanceProcessData objectPInstanceProcessData = new PInstanceProcessData();
    objectPInstanceProcessData.result = pInstance.getResult().toString();
    objectPInstanceProcessData.errormsg = pInstance.getErrorMsg();
    objectPInstanceProcessData.pMsg = "";
    vector.addElement(objectPInstanceProcessData);
    PInstanceProcessData pinstanceData[] = new PInstanceProcessData[1];
    vector.copyInto(pinstanceData);
    return pinstanceData;

  }

  protected String[] getDefaultValues(String... values) throws Exception {

    values[0] = searchDefaultValueCustom(values[0], "IsSalesOrderTransaction", 0);
    // Org DefaultValue for IDL is * and dont make sense for Invoices
    // values[1] = searchDefaultValueCustom(values[1], "Organization", 1);
    values[2] = searchDefaultValueCustom(values[2], "DocumentNo", 2);
    values[3] = searchDefaultValueCustom(values[3], "OrderReference", 3);
    values[4] = searchDefaultValueCustom(values[4], "DescriptionHeader", 4);
    values[5] = searchDefaultValueCustom(values[5], "TransactionDocument", 5);
    values[6] = searchDefaultValueCustom(values[6], "InvoiceDate", 6);
    values[7] = searchDefaultValueCustom(values[7], "AccountingDate", 7);
    values[8] = searchDefaultValueCustom(values[8], "TaxDate", 8);
    values[9] = searchDefaultValueCustom(values[9], "BusinessPartner", 9);
    values[10] = searchDefaultValueCustom(values[10], "UserContact", 10);
    values[11] = searchDefaultValueCustom(values[11], "PriceList", 11);
    values[12] = searchDefaultValueCustom(values[12], "SalesRepresentativeCompanyAgent", 12);
    values[13] = searchDefaultValueCustom(values[13], "PrintDiscount", 13);
    values[14] = searchDefaultValueCustom(values[14], "FormOfPayment", 14);
    values[15] = searchDefaultValueCustom(values[15], "PaymentsTerms", 15);
    values[16] = searchDefaultValueCustom(values[16], "Project", 16);
    values[17] = searchDefaultValueCustom(values[17], "FinancialInvoiceLine", 17);
    values[18] = searchDefaultValueCustom(values[18], "Account", 18);
    values[19] = searchDefaultValueCustom(values[19], "Product", 19);
    values[20] = searchDefaultValueCustom(values[20], "DescriptionLine", 20);
    values[21] = searchDefaultValueCustom(values[21], "InvoicedQuantity", 21);
    values[22] = searchDefaultValueCustom(values[22], "PriceListVersion", 22);
    values[23] = searchDefaultValueCustom(values[23], "NetUnitPrice", 23);
    values[24] = searchDefaultValueCustom(values[24], "NetListPrice", 24);
    values[25] = searchDefaultValueCustom(values[25], "Tax", 25);
    values[26] = searchDefaultValueCustom(values[26], "Process", 26);
    values[27] = searchDefaultValueCustom(values[27], "Withholding", 27);
    values[28] = searchDefaultValueCustom(values[28], "ExcludeForWithholding", 28);
    values[29] = searchDefaultValueCustom(values[29], "User1", 29);
    values[30] = searchDefaultValueCustom(values[30], "User2", 30);


    return values;
  }

  private String searchDefaultValueCustom(String value, String name, int i) throws Exception {
    // Custom method if is null return an empty string
    value = searchDefaultValue("Invoices", name, value);
    return (value == null ? "" : value);

  }

  private PriceListVersion getPriceListVersion(PriceList priceList, Date date) throws IOException,
      ServletException {

    OBCriteria PriceListVersionCriteria = OBDal.getInstance()
        .createCriteria(PriceListVersion.class);
    PriceListVersionCriteria.add(Expression.eq("priceList", priceList));
    PriceListVersionCriteria.add(Expression.le("validFromDate", date));
    PriceListVersionCriteria.addOrderBy("validFromDate", false);

    List<PriceListVersion> PLVersionList = PriceListVersionCriteria.list();
    if (PLVersionList.isEmpty()) {
      return null;
    } else {
      return PLVersionList.get(0);
    }

  }
}
