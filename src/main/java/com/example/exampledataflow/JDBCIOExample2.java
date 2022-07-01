package com.example.exampledataflow;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;


public class JDBCIOExample2 {

    public static final String BD_USER = "USER_MRCH_FRTR_PURCHASE_ORDER";
    public static final String BD_PASSWORD = "N9rLzdW4bg5rKf5LPHEsNqWp";
    public static final String ORG_POSTGRESQL_DRIVER = "org.postgresql.Driver";
    public static final String URL_CONNECTION_BD = "jdbc:postgresql://localhost:5433/PSQL_MRCH_FRTR_PURCHASE_ORDER";
    public static final String QUERY_PO_V2S = "SELECT " +
            " id, business_unit_code, business_unit_country_code,purchase_order_code_local, can_be_shipped, eta, etd, sku, vin, quantity" +
            " from purchase_order_v2s " +
            " WHERE 1 = ?";
    public static final String OUTPUT_JDBC_OUTPUT_CSV = "/Users/rarodriguezl/FalMerchandise/gitlab/ExampleDataFlow/src/main/resources/output/jdbc_output.csv";
    private static ObjectMapper objectMapper;

    static {
        objectMapper = new ObjectMapper();
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    public static void main(String[] args) {

        Pipeline p = Pipeline.create();

        PCollection<PurchaseOrderEntity> pPurchaseOrderEntityList = getPurchaseOrderEntity(p);

        PCollection <KV<String, PurchaseOrderEntity>>  pKv =   pPurchaseOrderEntityList.apply(ParDo.of(new StringToKV()));

        PCollection<KV<String , Iterable<PurchaseOrderEntity>>> pKvGroup =  pKv.apply(GroupByKey.<String, PurchaseOrderEntity>create());

        PCollection<PurchaseOrderAggregatorDto> poAgregatpr =  pKvGroup.apply(ParDo.of(new generatePoAgregator()));

        PCollection<String> output =  poAgregatpr.apply(ParDo.of(new KVToAggregatorString()));

        output.apply(TextIO.write().to(OUTPUT_JDBC_OUTPUT_CSV).withNumShards(1).withSuffix(".csv"));

        p.run();

    }

    static class generatePoAgregator extends DoFn<KV<String, Iterable<PurchaseOrderEntity>> , PurchaseOrderAggregatorDto> {

        @ProcessElement
        public void processElement(ProcessContext c){
             PurchaseOrderAggregatorDto purchaseOrderAggregatorDto = new PurchaseOrderAggregatorDto();
              List<PurchaseOrderAggregatorDto.Detail> details = new ArrayList<>();

              KV<String, Iterable<PurchaseOrderEntity>> input =  c.element();
              List<PurchaseOrderEntity> purchaseOrderEntityList = StreamSupport
                    .stream( input.getValue().spliterator(), true)
                    .collect(Collectors.toList());

            purchaseOrderEntityList.forEach(p-> {
                details.add(new PurchaseOrderAggregatorDto.Detail(p.getSku(),p.getVin()));
            });

            purchaseOrderAggregatorDto.setPurchaseOrderCodeLocal(input.getKey());
            purchaseOrderAggregatorDto.setProducts(details);

            purchaseOrderAggregatorDto.setBusinessUnit(PurchaseOrderAggregatorDto.BusinessUnit.builder()
                            .businessUnit(purchaseOrderEntityList.get(0).getBusinessUnitCode())
                            .businessUnitCountry(purchaseOrderEntityList.get(0).getBusinessUnitCountryCode())
                    .build());

              c.output(purchaseOrderAggregatorDto);
        }
    }

    static class KVToString extends DoFn<KV<String, Iterable<PurchaseOrderEntity>>, String> {

        @ProcessElement
        public void processElement(ProcessContext c){
            String strKey = c.element().getKey();
            Iterable<PurchaseOrderEntity> purchaseOrderEntities =   c.element().getValue();
            c.output(strKey+","+purchaseOrderEntities);
        }
    }

    static class KVToAggregatorString extends DoFn<PurchaseOrderAggregatorDto, String> {

        @ProcessElement
        public void processElement(ProcessContext c){
            PurchaseOrderAggregatorDto purchaseOrderAggregatorDto = c.element();
            c.output(purchaseOrderAggregatorDto.toString());
        }
    }



    private static PCollection<PurchaseOrderEntity> getPurchaseOrderEntity(Pipeline p) {
        return p.apply(JdbcIO.<PurchaseOrderEntity>read().
                withDataSourceConfiguration(JdbcIO.DataSourceConfiguration
                        .create(ORG_POSTGRESQL_DRIVER, URL_CONNECTION_BD)
                        .withUsername(BD_USER)
                        .withPassword(BD_PASSWORD))
                .withQuery(QUERY_PO_V2S)
                .withCoder(SerializableCoder.of(PurchaseOrderEntity.class))

                .withStatementPreparator((JdbcIO.StatementPreparator) preparedStatement -> preparedStatement.setLong(1, 1))
                .withRowMapper((JdbcIO.RowMapper<PurchaseOrderEntity>) resultSet -> {
                    PurchaseOrderEntity purchaseOrderEntity = new PurchaseOrderEntity();
                    purchaseOrderEntity.setId(resultSet.getString(1));
                    purchaseOrderEntity.setBusinessUnitCode(resultSet.getString(2));
                    purchaseOrderEntity.setBusinessUnitCountryCode(resultSet.getString(3));
                    purchaseOrderEntity.setPurchaseOrderCodeLocal(resultSet.getString(4));
                    purchaseOrderEntity.setCanBeShipped(resultSet.getBoolean(5));
                    purchaseOrderEntity.setEta(resultSet.getDate(6));
                    purchaseOrderEntity.setEtd(resultSet.getDate(7));
                    purchaseOrderEntity.setSku(resultSet.getString(8));
                    purchaseOrderEntity.setVin(resultSet.getString(9));
                    purchaseOrderEntity.setQuantity(resultSet.getLong(10));
                    return purchaseOrderEntity;
                })
        );
    }

   static class StringToKV extends DoFn<PurchaseOrderEntity, KV<String, PurchaseOrderEntity>> {

        @ProcessElement
        public void processElement(ProcessContext c){
              PurchaseOrderEntity input =  c.element();
             c.output(KV.of(input.getPurchaseOrderCodeLocal(), input));
        }
    }


    static class PurchaseOrderToKV extends DoFn<PurchaseOrderEntity, String> {
        @ProcessElement
        public void processElement(ProcessContext c) throws JsonProcessingException {
            PurchaseOrderEntity purchaseOrderEntity = c.element();
            String  data = objectMapper.writeValueAsString(purchaseOrderEntity);
            c.output(purchaseOrderEntity.getPurchaseOrderCodeLocal() +", "+data);
        }
    }
}
