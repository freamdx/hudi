/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.hudi.dml

import org.apache.spark.SparkConf
import org.apache.spark.sql.hudi.{CompositeKryoRegistrator, CompositeSessionExtension}
import org.apache.spark.sql.hudi.common.HoodieSparkSqlTestBase
import org.junit.jupiter.api.Assertions

class TestGeometry extends HoodieSparkSqlTestBase {
  protected lazy val typeFormats: Seq[String] = Seq("MOR", "COW")

  override def sparkConf(): SparkConf = {
    super.sparkConf()
      .set("spark.kryo.registrator", classOf[CompositeKryoRegistrator].getName)
      .set("spark.sql.extensions", classOf[CompositeSessionExtension].getName)
  }

  test("DDL: create table with geometry") {
    typeFormats.foreach { format =>
      withTempDir { dir =>
        // CREATE TABLE
        val tab1 = generateTableName
        spark.sql(
          s"""
             |CREATE TABLE $tab1 (id INT NOT NULL, geom GEOMETRY NOT NULL, h3 INT)
             |USING hudi
             |TBLPROPERTIES ( 'primaryKey'='id', 'type'='$format' )
             |""".stripMargin)
        val schema = spark.table(tab1).schema
        Assertions.assertEquals(schema.size, 8)
        Assertions.assertFalse(schema("id").nullable)
        Assertions.assertEquals(schema("geom").dataType.typeName, "geometry")

        // CREATE TABLE AS
        val tab2 = generateTableName
        spark.sql(
          s"""
             |CREATE TABLE $tab2
             |USING hudi
             |PARTITIONED BY (h3)
             |TBLPROPERTIES ( 'primaryKey'='id', 'type'='$format' )
             |AS SELECT 1 as id, ST_GeomFromText('POINT(1 2 3)') as geom, 10 as h3
             |""".stripMargin)
        Assertions.assertEquals(
          spark.table(tab2).schema("geom").dataType.typeName, "geometry"
        )
      }
    }
  }

  test("DDL: create table with geometry, select") {
    typeFormats.foreach { format =>
      withTempDir { dir =>
        val tab1 = generateTableName
        spark.sql(
          s"""
             |CREATE TABLE $tab1 (id INT, geom GEOMETRY, h3 INT)
             |USING hudi
             |TBLPROPERTIES ( 'primaryKey'='id', 'type'='$format' )
             |""".stripMargin)
        select(tab1)

        val tab2 = generateTableName
        spark.sql(
          s"""
             |CREATE TABLE $tab2 (id INT, geom GEOMETRY, h3 INT)
             |USING hudi
             |PARTITIONED BY (h3)
             |TBLPROPERTIES ( 'primaryKey'='id', 'type'='$format' )
             |""".stripMargin)
        select(tab2)
      }
    }
  }

  test("DDL: create table with geometry, simple dml") {
    typeFormats.foreach { format =>
      withTempDir { dir =>
        val tab1 = generateTableName
        spark.sql(
          s"""
             |CREATE TABLE $tab1
             |USING hudi
             |LOCATION '${dir.getCanonicalPath}/${tab1}'
             |TBLPROPERTIES ( 'primaryKey'='id', 'type'='$format', preCombineField = 'h3' )
             |AS SELECT 1 as id, ST_GeomFromText('POINT(1 2 3)') as geom, 1 as h3
             |""".stripMargin)
        dml(tab1)

        val tab2 = generateTableName
        spark.sql(
          s"""
             |CREATE TABLE $tab2
             |USING hudi
             |PARTITIONED BY (h3)
             |LOCATION '${dir.getCanonicalPath}/${tab2}'
             |TBLPROPERTIES ( 'primaryKey'='id', 'type'='$format', preCombineField = 'h3' )
             |AS SELECT 1 as id, ST_GeomFromText('POINT(1 2 3)') as geom, 1 as h3
             |""".stripMargin)
        dml(tab2)
      }
    }
  }

  test("DDL: create table with geometry, simple merge") {
    typeFormats.foreach { format =>
      withTempDir { dir =>
        val tab1 = generateTableName
        spark.sql(
          s"""
             |CREATE TABLE $tab1 (id INT, geom GEOMETRY, h3 INT)
             |USING hudi
             |LOCATION '${dir.getCanonicalPath}/${tab1}'
             |TBLPROPERTIES ( 'primaryKey'='id', 'type'='$format', preCombineField = 'h3' )
             |""".stripMargin)

        val tab2 = generateTableName
        spark.sql(
          s"""
             |CREATE TABLE $tab2 (id INT, geom GEOMETRY, h3 INT)
             |USING hudi
             |LOCATION '${dir.getCanonicalPath}/${tab2}'
             |TBLPROPERTIES ( 'primaryKey'='id', 'type'='$format', preCombineField = 'h3' )
             |""".stripMargin)

        // MERGE INTO
        spark.sql(
          s"""
             |INSERT INTO $tab1
             |SELECT 1, ST_GeomFromText('POINT(1 2)'), 1
             |""".stripMargin)
        spark.sql(
          s"""
             |INSERT INTO $tab2 VALUES
             |( 1, ST_GeomFromText('POINT(0 1)'), 2 ),
             |( 2, ST_GeomFromText('POINT(2 1)'), 3 )
             |""".stripMargin)

        spark.sql(
          s"""
             |MERGE INTO $tab1
             |USING $tab2
             |ON $tab1.id = $tab2.id
             |WHEN MATCHED THEN
             |UPDATE SET h3 = $tab2.h3, geom = $tab2.geom
             |WHEN NOT MATCHED THEN
             |INSERT *
             |""".stripMargin)
        Assertions.assertEquals(spark.table(tab1).count(), 2)
        val col = spark.sql(s"select h3 from $tab1").sort("h3").collect()
        Assertions.assertEquals(col.mkString, "[2][3]")
      }
    }
  }

  private def select(tab: String) = {
    // INSERT
    spark.sql(
      s"""
         |INSERT INTO $tab VALUES
         |( 1, ST_GeomFromText('POINT(0 1)'), 1 ),
         |( 2, ST_GeomFromText('POINT(1 2)'), 2 ),
         |( 3, ST_GeomFromText('POINT(1 1)'), 3 ),
         |( 4, ST_GeomFromText('POINT(2 2)'), 4 ),
         |( 5, ST_GeomFromText('POINT(3 3 1)'), 5 )
         |""".stripMargin)

    // INSERT SELECT
    spark.sql(
      s"""
         |INSERT INTO $tab
         |SELECT 4, ST_GeomFromText('POINT(4 4)'), 4
         |""".stripMargin)
    val count1 = spark.sql(s"select * from $tab").count()

    val count2 = spark.sql(
      s"""
         |SELECT * FROM $tab
         |WHERE ST_Intersects( geom, ST_GeomFromText('POINT(1 1)') )
         |""".stripMargin).count()
    Assertions.assertEquals(count2, 1)

    val count3 = spark.sql(
      s"""
         |SELECT * FROM $tab
         |WHERE ST_Contains( ST_GeomFromText('LINESTRING(0 0, 3 3)'), geom )
         |""".stripMargin).count()
    Assertions.assertEquals(count3, 2)

    // INSERT OVERWRITE
    spark.sql(
      s"""
         |INSERT OVERWRITE $tab
         |SELECT 0, ST_GeomFromText('POINT(3 2 1)'), 0
         |""".stripMargin)
    val count4 = spark.sql(s"select * from $tab").count()
    Assertions.assertTrue(count1 > count4)
  }

  private def dml(tab: String) = {
    // INSERT
    spark.sql(
      s"""
         |INSERT INTO $tab VALUES
         |( 2, ST_GeomFromText('POINT(0 1)'), 2 ),
         |( 3, ST_GeomFromText('POINT(1 2)'), 3 ),
         |( 4, ST_GeomFromText('POINT(1 1)'), 4 ),
         |( 5, ST_GeomFromText('POINT(2 2)'), 5 ),
         |( 6, ST_GeomFromText('POINT(3 3 1)'), 6 )
         |""".stripMargin)
    val count1 = spark.sql(s"select * from $tab").count()

    // DELETE
    spark.sql(
      s"""
         |DELETE FROM $tab
         |WHERE ST_Intersects( geom, ST_GeomFromText('POINT(1 2)') )
         |""".stripMargin)
    val count2 = spark.sql(s"select * from $tab").count()
    Assertions.assertTrue(count1 > count2)

    // UPDATE
    spark.sql(
      s"""
         |UPDATE $tab
         |SET geom = ST_GeomFromText('POINT(3 2 1)')
         |WHERE id=2
         |""".stripMargin)
    val col = spark.sql(s"select geom from $tab where id=2").collect()
    Assertions.assertEquals(col(0).get(0).toString, "POINT (3 2)")
  }

}
