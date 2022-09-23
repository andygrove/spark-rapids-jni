/*
 * Copyright (c) 2022, NVIDIA CORPORATION.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nvidia.spark.rapids.jni;

import ai.rapids.cudf.ColumnVector;
import ai.rapids.cudf.DType;
import ai.rapids.cudf.Table;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class RegexTest {

  @Test
  void regexStabilityTest1() throws InterruptedException {
    doStabilityTest(450_000_000, 4, 10);
  }

  void doStabilityTest(int n, int numThreads, int maxAttempt) throws InterruptedException {
    // This test aims to reproduce the following Spark query, which produces inconsistent results between runs
    // spark.range(1000000000L).selectExpr("CAST(id as STRING) as str_id").filter("regexp_like(str_id, '(.|\n)*1(.|\n)0(.|\n)*')").count()

    // no need to create initial input in parallel
    ColumnVector inputs[] = new ColumnVector[numThreads];
    int chunk_size = n/numThreads;
    for (int j = 0; j<numThreads; j++) {
      long array[] = new long[chunk_size];
      for (int i = 0; i < chunk_size; i++) {
        array[i] = chunk_size * j + i;
      }
      inputs[j] = ColumnVector.fromLongs(array);
    }

    String csvResults[] = new String[maxAttempt];

    for (int attempt = 0; attempt< maxAttempt; attempt++) {
      System.err.println("attempt " + attempt + " of " + maxAttempt);

      Thread threads[] = new Thread[numThreads];
      long result[] = new long[numThreads];
      for (int j = 0; j < numThreads; j++) {
        int threadNo = j;
        threads[j] = new Thread(() -> {
          try (ColumnVector castTo = inputs[threadNo].castTo(DType.STRING);
               ColumnVector matchesRe = castTo.matchesRe("(.|\\n)*1(.|\\n)0(.|\\n)*");
               Table t = new Table(inputs[threadNo]);
               Table t2 = t.filter(matchesRe)) {
            result[threadNo] = t2.getRowCount();
            System.err.println("thread " + threadNo + " count: " + t2.getRowCount());
          }

        });
        System.err.println("starting thread");
        threads[j].start();
      }

      for (Thread t : threads) {
        System.err.println("waiting for thread");
        t.join();
        System.err.println("thread completed");
      }

      csvResults[attempt] = Arrays.stream(result)
              .mapToObj(String::valueOf)
              .collect(Collectors.joining(","));

      System.err.println("Results: " + csvResults[attempt]);
    }

    // check that the results from each run are the same
    for (int attempt = 0; attempt< maxAttempt; attempt++) {
      assertEquals(csvResults[0], csvResults[attempt]);
    }

  }

}
