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

import ai.rapids.cudf.AssertUtils;
import ai.rapids.cudf.ColumnVector;
import ai.rapids.cudf.DType;
import ai.rapids.cudf.Table;
import com.nvidia.spark.rapids.jni.CastException;
import org.junit.jupiter.api.Test;

import java.math.RoundingMode;
import java.util.stream.IntStream;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class CastStringsTest {
  @Test
  void castToIntegerTest() {
    Table.TestBuilder tb = new Table.TestBuilder();
    tb.column(3l, 9l, 4l, 2l, 20l, null, null);
    tb.column(5, 1, 0, 2, 7, null, null);
    tb.column(new Byte[]{2, 3, 4, 5, 9, null, null});
    Table expected = tb.build();

    Table.TestBuilder tb2 = new Table.TestBuilder();
    tb2.column("3", "9", "4", "2", "20", null, "7.6asd");
    tb2.column("5", "1", "0", "2", "7", null, "asdf");
    tb2.column("2", "3", "4", "5", "9", null, "7.8.3");

    List<ColumnVector> result = new ArrayList<>();

    try (Table origTable = tb2.build()) {
      for (int i = 0; i < origTable.getNumberOfColumns(); i++) {
        ColumnVector string_col = origTable.getColumn(i);
        result.add(CastStrings.toInteger(string_col, false, 
                   expected.getColumn(i).getType()));
      }
      Table result_tbl = new Table(
        result.toArray(new ColumnVector[result.size()]));
      AssertUtils.assertTablesAreEqual(expected, result_tbl);
    }
  }

  @Test
  void castThenRegexStabilityRepro() throws InterruptedException {
    // spark.range(1000000000L).selectExpr("CAST(id as STRING) as str_id").filter("regexp_like(str_id, '(.|\n)*1(.|\n)0(.|\n)*')").count()

    int n = 1_000_000_000;
    int numThreads = 2;
    long expectedValues[] = new long [] { 0, 0 }; // expected values TBD

    // no need to create initial input in parallel
    ColumnVector inputs[] = new ColumnVector[numThreads];
    for (int j = 0; j<numThreads; j++) {
      long array[] = new long[n];
      int chunk_size = n/numThreads;
      for (int i = 0; i < n; i++) {
        array[i] = chunk_size * j + i;
      }
      try (ColumnVector cv = ColumnVector.fromLongs(array)) {
        inputs[j] = cv.castTo(DType.STRING);
      }
    }

    int maxAttempt = 10;
    for (int attempt = 0; attempt< maxAttempt; attempt++) {
      System.out.println("attempt " + attempt + " of " + maxAttempt);

      Thread threads[] = new Thread[numThreads];
      long result[] = new long[numThreads];
      for (int j = 0; j < numThreads; j++) {
        int threadNo = j;
        threads[j] = new Thread(() -> {
          try (ColumnVector matchesRe = inputs[threadNo].matchesRe("(.|\\n)*1(.|\\n)0(.|\\n)*");
               Table t = new Table(inputs[threadNo]);
               Table t2 = t.filter(matchesRe)) {
            result[threadNo] = t2.getRowCount();
            System.out.println("thread " + threadNo + " count: " + t2.getRowCount());
          }

        });
        System.out.println("starting thread");
        threads[j].start();
      }

      for (Thread t : threads) {
        System.out.println("waiting for thread");
        t.join();
        System.out.println("thread completed");
      }

      for (int j = 0; j < numThreads; j++) {
        assertEquals(expectedValues[j], result[j]);
      }

      // pause between attempts
      Thread.sleep(500);
    }
  }

  @Test
  void castToIntegerAnsiTest() {
    Table.TestBuilder tb = new Table.TestBuilder();
    tb.column(3l, 9l, 4l, 2l, 20l);
    tb.column(5, 1, 0, 2, 7);
    tb.column(new Byte[]{2, 3, 4, 5, 9});
    Table expected = tb.build();

    Table.TestBuilder tb2 = new Table.TestBuilder();
    tb2.column("3", "9", "4", "2", "20");
    tb2.column("5", "1", "0", "2", "7");
    tb2.column("2", "3", "4", "5", "9");

    List<ColumnVector> result = new ArrayList<>();

    try (Table origTable = tb2.build()) {
      for (int i = 0; i < origTable.getNumberOfColumns(); i++) {
        ColumnVector string_col = origTable.getColumn(i);
        result.add(CastStrings.toInteger(string_col, true, 
                   expected.getColumn(i).getType()));
      }
      Table result_tbl = new Table(
        result.toArray(new ColumnVector[result.size()]));
      AssertUtils.assertTablesAreEqual(expected, result_tbl);
    }

    Table.TestBuilder fail = new Table.TestBuilder();
    fail.column("asdf", "9.0.2", "- 4e", "b2", "20-fe");

    try {
        Table failTable = fail.build();
        CastStrings.toInteger(failTable.getColumn(0), true,
                              expected.getColumn(0).getType());
        fail("Should have thrown");
      } catch (CastException e) {
        assertEquals("asdf", e.getStringWithError());
        assertEquals(0, e.getRowWithError());
    }
  }
}
