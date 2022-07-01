package com.example.exampledataflow;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class LoadPurchaseOrder {

    private static ObjectMapper objectMapper;

    static {
        objectMapper = new ObjectMapper();
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    public static void main(String[] args) {
        System.out.println("MAIN");
        Pipeline p = Pipeline.create();
        PCollection<PurchaseOrderV2SEntity> pPurchaseOrderV2SEntity = getPurchaseOrderV2SEntity(p);

        PCollection<String> pList=  pPurchaseOrderV2SEntity.apply(ParDo.of(new PurchaseOrderToKV()));
        pList.apply(TextIO.write().to("/Users/rarodriguezl/FalMerchandise/gitlab/ExampleDataFlow/src/main/resources/output/jdbc_output_v2s.csv").withNumShards(1).withSuffix(".csv"));

        System.out.println("FIN*******************************************++");
    }

    private static PCollection<PurchaseOrderV2SEntity> getPurchaseOrderV2SEntity(Pipeline p) {
        return p.apply(JdbcIO.<PurchaseOrderV2SEntity>read().
                withDataSourceConfiguration(JdbcIO.DataSourceConfiguration
                        .create("org.postgresql.Driver", "jdbc:postgresql://localhost:5433/PSQL_MRCH_FRTR_PURCHASE_ORDER")
                        .withUsername("USER_MRCH_FRTR_PURCHASE_ORDER")
                        .withPassword("N9rLzdW4bg5rKf5LPHEsNqWp"))
                .withQuery("SELECT " +
                        " id, business_unit_code, business_unit_country_code " +
               //         " sku, vin, quantity, remaining_quantity, etd, eta " +
                        " from purchase_order_v2s where 1 = ?")
                .withCoder(SerializableCoder.of(PurchaseOrderV2SEntity.class))

                .withStatementPreparator(new JdbcIO.StatementPreparator() {
                    public void setParameters(PreparedStatement preparedStatement) throws Exception {
                        preparedStatement.setLong(1, 1);
                    }
                })
                .withRowMapper(new JdbcIO.RowMapper<PurchaseOrderV2SEntity>() {

                    public PurchaseOrderV2SEntity mapRow(ResultSet resultSet) throws Exception {
                        PurchaseOrderV2SEntity purchaseOrderEntity = new PurchaseOrderV2SEntity();
                        purchaseOrderEntity.setId(resultSet.getString(1));
                        purchaseOrderEntity.setBusinessUnitCode(resultSet.getString(2));
                        purchaseOrderEntity.setBusinessUnitCountryCode(resultSet.getString(3));
//                        purchaseOrderEntity.setPurchaseOrderCodeLocal(resultSet.getString(4));
//                        purchaseOrderEntity.setCanBeShipped(resultSet.getBoolean(5));
//
//                        purchaseOrderEntity.setSku(resultSet.getString(6));
//                        purchaseOrderEntity.setVin(resultSet.getString(7));
//
//                        purchaseOrderEntity.setQuantity(resultSet.getLong(8));
//                        purchaseOrderEntity.setQuantityRemaining(resultSet.getLong(9));
//
//                        purchaseOrderEntity.setEta(resultSet.getDate(10));
//                        purchaseOrderEntity.setEtd(resultSet.getDate(11));

                        System.out.println(purchaseOrderEntity);
                        return purchaseOrderEntity;
                    }
                })
        );
    }

    static class PurchaseOrderToKV extends DoFn<PurchaseOrderV2SEntity, String> {
        @ProcessElement
        public void processElement(ProcessContext c) throws JsonProcessingException {
            PurchaseOrderV2SEntity purchaseOrderEntity = c.element();
            String  data = objectMapper.writeValueAsString(purchaseOrderEntity);
            c.output(purchaseOrderEntity.getPurchaseOrderCodeLocal() +", "+data);
        }
    }
}
