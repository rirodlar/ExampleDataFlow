package com.example.exampledataflow;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.TypeDescriptors;

import java.util.List;

public class InMemoryExample {

    public static void main(String[] args) {

        MyOptions myOptions = PipelineOptionsFactory.fromArgs(args).withValidation().as(MyOptions.class);

        Pipeline p = Pipeline.create(myOptions);

        PCollection<PurchaseOrderEntity> pPurchaseOrderList1 = p.apply(Create.of(getOPurchaseOrder()));

        PCollection<PurchaseOrderEntity> pPurchaseOrderList2 = p.apply(Create.of(getOPurchaseOrder2()));

        PCollectionList<PurchaseOrderEntity> list = PCollectionList.of(pPurchaseOrderList1).and(pPurchaseOrderList2);

       PCollection<PurchaseOrderEntity> merged =  list.apply(Flatten.pCollections());

        PCollectionList<PurchaseOrderEntity> partition = merged.apply(Partition.of(0, new MyPurchaseOrderPartition()));

       PCollection<PurchaseOrderEntity> pStrListFilterQty =  merged.apply(ParDo.of(new PurchaseOrderFilter()));

        PCollection<String> pCompanyListList =  pStrListFilterQty.apply(MapElements.into(TypeDescriptors.strings()).via((PurchaseOrderEntity c )-> c.getBusinessUnitCode()));

        PCollection<String> pCompanyListUpper =  pCompanyListList.apply(MapElements.into(TypeDescriptors.strings()).via((String c )-> c.toUpperCase()));

        PCollection<String> pfunctionCompanySodimac =  pCompanyListUpper.apply(MapElements.via(new Company()));

        PCollection<String> pfunctionCompanySodimacFilter =  pfunctionCompanySodimac.apply(Filter.by(new MyFilter()));

       //

        pfunctionCompanySodimacFilter.apply(TextIO.write().to(myOptions.getOutPutFile()).withNumShards(1).withSuffix(myOptions.getExtn()));

        p.run();

    }


    static class PurchaseOrderFilter extends DoFn<PurchaseOrderEntity, PurchaseOrderEntity>{
        @ProcessElement
        public void processElement(ProcessContext c){
            PurchaseOrderEntity purchaseOrderEntity = c.element();
            if(purchaseOrderEntity.getQuantityRemaining() > 0) {
                c.output(purchaseOrderEntity);
            }
        }
    }

   static class Company extends SimpleFunction<String, String>{

        @Override
        public String apply(String input) {

                if(input.equals("SOD")){
                    return "SODIMAC";
                }else{
                    return input;
                }
        }
    }

    static List<PurchaseOrderEntity> getOPurchaseOrder(){
        PurchaseOrderEntity c1 = new PurchaseOrderEntity("1", "sod", "11", 100L);
        PurchaseOrderEntity c2 = new PurchaseOrderEntity("2", "sod", "12", 0L);
        PurchaseOrderEntity c3 = new PurchaseOrderEntity("3", "fab", "13", 100L);

        return List.of(c1,c2, c3);
    }

    static List<PurchaseOrderEntity> getOPurchaseOrder2(){
        PurchaseOrderEntity c1 = new PurchaseOrderEntity("4", "sod", "11", 100L);
        PurchaseOrderEntity c2 = new PurchaseOrderEntity("5", "sod", "12", 0L);

        return List.of(c1,c2);
    }


}

class MyFilter implements SerializableFunction<String, Boolean> {
    @Override
    public Boolean apply(String input) {
        return input.contains("SODIMAC");
    }
}

class MyPurchaseOrderPartition implements Partition.PartitionFn<PurchaseOrderEntity>{

    @Override
    public int partitionFor(PurchaseOrderEntity elem, int numPartitions) {
        if(elem.getQuantityRemaining() > 0){
            return 0; //particion POS[0] purchaseOrder Qty > 0
        }else {
            return 1; ////particion POS[1] purchaseOrder Qty == 0
        }
    }
}


