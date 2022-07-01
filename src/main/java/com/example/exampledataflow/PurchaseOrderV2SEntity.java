package com.example.exampledataflow;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.sql.Date;


public class PurchaseOrderV2SEntity implements Serializable {

    private String id;

    private String businessUnitCode;

    private String businessUnitCountryCode;

    private String purchaseOrderCodeLocal;

    private Date etd;

    private Date eta;

    private Long quantity;

    private Long quantityRemaining;

    private Boolean canBeShipped;

    private String sku;

    private String vin;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getBusinessUnitCode() {
        return businessUnitCode;
    }

    public void setBusinessUnitCode(String businessUnitCode) {
        this.businessUnitCode = businessUnitCode;
    }

    public String getBusinessUnitCountryCode() {
        return businessUnitCountryCode;
    }

    public void setBusinessUnitCountryCode(String businessUnitCountryCode) {
        this.businessUnitCountryCode = businessUnitCountryCode;
    }

    public String getPurchaseOrderCodeLocal() {
        return purchaseOrderCodeLocal;
    }

    public void setPurchaseOrderCodeLocal(String purchaseOrderCodeLocal) {
        this.purchaseOrderCodeLocal = purchaseOrderCodeLocal;
    }

    public Date getEtd() {
        return etd;
    }

    public void setEtd(Date etd) {
        this.etd = etd;
    }

    public Date getEta() {
        return eta;
    }

    public void setEta(Date eta) {
        this.eta = eta;
    }

    public Long getQuantity() {
        return quantity;
    }

    public void setQuantity(Long quantity) {
        this.quantity = quantity;
    }

    public Long getQuantityRemaining() {
        return quantityRemaining;
    }

    public void setQuantityRemaining(Long quantityRemaining) {
        this.quantityRemaining = quantityRemaining;
    }

    public Boolean getCanBeShipped() {
        return canBeShipped;
    }

    public void setCanBeShipped(Boolean canBeShipped) {
        this.canBeShipped = canBeShipped;
    }

    public String getSku() {
        return sku;
    }

    public void setSku(String sku) {
        this.sku = sku;
    }

    public String getVin() {
        return vin;
    }

    public void setVin(String vin) {
        this.vin = vin;
    }
}
