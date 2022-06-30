package com.example.exampledataflow;

import java.sql.PreparedStatement;
import java.sql.ResultSet;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.values.PCollection;


public class JDBCIOExample{

    public static void main(String[] args) {

        Pipeline p = Pipeline.create();

        PCollection<String> poutput = p.apply(JdbcIO.<String>read().
                withDataSourceConfiguration(JdbcIO.DataSourceConfiguration
                        .create("org.postgresql.Driver","jdbc:postgresql://localhost:5433/PSQL_MRCH_FRTR_BOOKING_RQ")
                        .withUsername("USER_MRCH_FRTR_BOOKING_RQ")
                        .withPassword("uw9rCkaMbYmPVurNLbej2d9L"))
                .withQuery("SELECT code, name, active from incoterm WHERE active = ? ")
                .withCoder(StringUtf8Coder.of())
                .withStatementPreparator(new JdbcIO.StatementPreparator() {

                    public void setParameters(PreparedStatement preparedStatement) throws Exception {
                        preparedStatement.setBoolean(1, true);
                    }
                })
                .withRowMapper(new JdbcIO.RowMapper<String>() {

                    public String mapRow(ResultSet resultSet) throws Exception {
                        return resultSet.getString(1)+","+resultSet.getString(2)+","+resultSet.getString(3);
                    }
                })
        );




        poutput.apply(TextIO.write().to("/Users/rarodriguezl/FalMerchandise/gitlab/ExampleDataFlow/src/main/resources/jdbc_output.csv").withNumShards(1).withSuffix(".csv"));

        p.run();
    }

}
