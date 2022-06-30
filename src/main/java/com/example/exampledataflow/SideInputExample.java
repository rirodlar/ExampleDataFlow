package com.example.exampledataflow;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.*;

import java.util.List;
import java.util.Map;

public class SideInputExample {

    public static void main(String[] args) {


        MyOptions myOptions = PipelineOptionsFactory.fromArgs(args).withValidation().as(MyOptions.class);

        Pipeline p = Pipeline.create(myOptions);

        PCollection<KV<String, String>> pReturn =  p.apply(TextIO.read().from("/Users/rarodriguezl/FalMerchandise/gitlab/ExampleDataFlow/src/main/resources/return.csv"))
                        .apply(ParDo.of(new DoFn<String, KV<String,String>>() {

                            @ProcessElement
                             public void process(ProcessContext c){
                                 String arr[] = c.element().split(",");
                                 c.output(KV.of(arr[0], arr[1]));
                             }

                        }));



        PCollectionView<Map<String, String>> pMap = pReturn.apply(View.asMap());

        PCollection<String> pCustList = p.apply(TextIO.read().from("/Users/rarodriguezl/FalMerchandise/gitlab/ExampleDataFlow/src/main/resources/cust_order.csv"));

        pCustList.apply(ParDo.of(new DoFn<String, Void>() {
            @ProcessElement
            public void process(ProcessContext c){
                Map<String, String> psideInputView = c.sideInput(pMap);
                String arr[] = c.element().split(",");
                String custName = psideInputView.get(arr[0]);
                    if(custName ==null) {
                        System.out.println(custName);
                    }
            }
        }).withSideInputs(pMap));

       // pReturn.apply(TextIO.write().to("myOptions.getOutPutFile()").withNumShards(1).withSuffix(".csv"));
       // pReturn.apply(TextIO.write().to(myOptions.getOutPutFile()).withNumShards(1).withSuffix(myOptions.getExtn()));

        p.run();




    }





}



