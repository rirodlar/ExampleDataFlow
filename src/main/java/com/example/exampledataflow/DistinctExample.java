package com.example.exampledataflow;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Distinct;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;

import java.util.Map;

public class DistinctExample {

    public static void main(String[] args) {


      Pipeline p = Pipeline.create();

      PCollection<String> pCustList = p.apply(TextIO.read().from("/Users/rarodriguezl/FalMerchandise/gitlab/ExampleDataFlow/src/main/resources/Distinct.csv"));

      PCollection<String> uniqueCust = pCustList.apply(Distinct.<String>create());

        uniqueCust.apply(TextIO.write().to("/Users/rarodriguezl/FalMerchandise/gitlab/ExampleDataFlow/src/main/resources/distinct_out.csv")
                        .withNumShards(1).withSuffix(".csv"));

        p.run();


    }





}



