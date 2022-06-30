package com.example.exampledataflow;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.values.PCollection;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class ExampleDataFlowApplication {

    public static void main(String[] args) {
       // SpringApplication.run(ExampleDataFlowApplication.class, args);

        Pipeline pipeline = Pipeline.create();

        PCollection<String> output = pipeline.apply(TextIO.read().from("/Users/rarodriguezl/FalMerchandise/gitlab/ExampleDataFlow/src/main/resources/input.csv"));

        output.apply(TextIO.write().to("/Users/rarodriguezl/FalMerchandise/gitlab/ExampleDataFlow/src/main/resources/output.csv").withNumShards(1).withSuffix(".csv"));


        pipeline.run();
    }

}
