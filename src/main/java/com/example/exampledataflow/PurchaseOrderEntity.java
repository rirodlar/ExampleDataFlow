package com.example.exampledataflow;

import java.io.Serializable;

public class PurchaseOrderEntity implements Serializable {

    private String id;
    private String businessUnitCode;

    private String purchaseOrderCodeLocal;

    private Long quantityRemaining;


    public PurchaseOrderEntity() {
    }

    public PurchaseOrderEntity(String id, String businessUnitCode, String purchaseOrderCodeLocal, Long quantityRemaining) {
        this.id = id;
        this.businessUnitCode = businessUnitCode;
        this.purchaseOrderCodeLocal = purchaseOrderCodeLocal;
        this.quantityRemaining = quantityRemaining;
    }

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

    public String getPurchaseOrderCodeLocal() {
        return purchaseOrderCodeLocal;
    }

    public void setPurchaseOrderCodeLocal(String purchaseOrderCodeLocal) {
        this.purchaseOrderCodeLocal = purchaseOrderCodeLocal;
    }

    public Long getQuantityRemaining() {
        return quantityRemaining;
    }

    public void setQuantityRemaining(Long quantityRemaining) {
        this.quantityRemaining = quantityRemaining;
    }
}
