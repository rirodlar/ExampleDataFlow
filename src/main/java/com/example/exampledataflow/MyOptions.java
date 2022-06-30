package com.example.exampledataflow;

import org.apache.beam.sdk.options.PipelineOptions;

public interface MyOptions extends PipelineOptions {

    void setInputFile(String file);
    String getInputFile();

    void setOutPutFile(String file);
    String getOutPutFile();

    void setExtn(String extn);
    String getExtn();


}
