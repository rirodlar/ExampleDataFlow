package com.example.exampledataflow;

import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.sql.Date;

@NoArgsConstructor
@Data
public class PurchaseOrderEntity implements Serializable {

    private String id;
    private String businessUnitCode;

    private String businessUnitCountryCode;

    private String purchaseOrderCodeLocal;

    private Date etd;

    private Date eta;

    private Long quantityRemaining;

    private Boolean canBeShipped;

    private String sku;

    private String vin;

    private Long quantity;


    public PurchaseOrderEntity(String id, String company, String purchaseOrderCodeLocal, long quantityRemaining) {
        this.id = id;
        this.businessUnitCode = company;
        this.purchaseOrderCodeLocal = purchaseOrderCodeLocal;
        this.quantityRemaining = quantityRemaining;
    }

}
