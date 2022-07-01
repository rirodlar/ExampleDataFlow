package com.example.exampledataflow;

import lombok.Builder;
import lombok.Data;

import java.io.Serializable;
import java.util.List;

@Data
public class PurchaseOrderAggregatorDto implements Serializable {

    private String purchaseOrderCodeLocal;
    private BusinessUnit businessUnit;
    private List<Detail> products;


    @Builder
    @Data
    public static class Detail implements Serializable {
        private String sku;
        private String vin;
    }

    @Builder
    @Data
    public static class BusinessUnit implements Serializable{

        private String businessUnit;
        private String businessUnitCountry;

    }

    @Builder
    @Data
    public static class PurchaseOrderInfo implements Serializable {
        private String eta;
        private String etd;
        private String canBeShipped;
    }


}
