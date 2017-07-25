package com.datawizards.sparkschema2class

import java.io.File
import java.sql.Timestamp

import com.datawizards.sparkschema2class.TestModel._
import org.apache.spark.sql.SparkSession
import org.scalatest.{FunSuite, Matchers}

class ClassGeneratorTest extends FunSuite with Matchers {
  private lazy val session = SparkSession.builder().master("local").getOrCreate()

  test("Generate based on json file") {
    val df = session.read.json("src/test/resources/people.json")
    val expected =
      """package com.datawizards
        |
        |/**
        |  * Generated automatically.
        |  */
        |case class Person(
        |  age: Option[Long],
        |  name: Option[String]
        |)""".stripMargin

    ClassGenerator.generateClass(df, "com.datawizards", "Person").replace("\n","").replace("\r","") should equal(expected.replace("\n","").replace("\r",""))
  }

  test("Generate based on CSV file") {
    val df = session
      .read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("src/test/resources/people.csv")
    val expected =
      """package com.datawizards
        |
        |/**
        |  * Generated automatically.
        |  */
        |case class Person(
        |  name: Option[String],
        |  title: Option[String],
        |  age: Option[Int],
        |  salary: Option[Double]
        |)""".stripMargin

    ClassGenerator.generateClass(df, "com.datawizards", "Person").replace("\n","").replace("\r","") should equal(expected.replace("\n","").replace("\r",""))
  }

  test("Generate to directory") {
    val df = session.read.json("src/test/resources/people.json")
    val expected =
      """package com.datawizards
        |
        |/**
        |  * Generated automatically.
        |  */
        |case class Person(
        |  age: Option[Long],
        |  name: Option[String]
        |)""".stripMargin.replace("\n","").replace("\r","")

    deleteDirectory("target/com")
    ClassGenerator.generateClassToDirectory(df, "com.datawizards", "Person", "target")

    readFileContent("target/com/datawizards/Person.scala").replace("\n","").replace("\r","") should equal(expected)
  }

  test("Generate all simple types") {
    val df = session.createDataFrame(Seq(
      ClassWithAllSimpleTypes(
        "text",
        1,
        1L,
        1.0,
        1.0f,
        1,
        true,
        1,
        new java.sql.Date(10000000),
        new Timestamp(10000000)
      )
    ))
    val expected =
      """package com.datawizards
        |
        |/**
        |  * Generated automatically.
        |  */
        |case class ClassWithAllSimpleTypes(
        |  strVal: Option[String],
        |  intVal: Int,
        |  longVal: Long,
        |  doubleVal: Double,
        |  floatVal: Float,
        |  shortVal: Short,
        |  booleanVal: Boolean,
        |  byteVal: Byte,
        |  dateVal: Option[java.sql.Date],
        |  timestampVal: Option[java.sql.Timestamp]
        |)""".stripMargin

    ClassGenerator.generateClass(df, "com.datawizards", "ClassWithAllSimpleTypes").replace("\n","").replace("\r","") should equal(expected.replace("\n","").replace("\r",""))
  }

  test("Generate array") {
    val df = session.createDataFrame(Seq(
      CV(Seq("1", "2"), Seq(1, 2, 3))
    ))
    val expected =
      """package com.datawizards
        |
        |/**
        |  * Generated automatically.
        |  */
        |case class CV(
        |  skills: Seq[String],
        |  grades: Seq[Int]
        |)""".stripMargin

    ClassGenerator.generateClass(df, "com.datawizards", "CV").replace("\n","").replace("\r","") should equal(expected.replace("\n","").replace("\r",""))
  }

  test("Generate nested array") {
    val df = session.createDataFrame(Seq(
      NestedArray(Seq(Seq("")), Seq(Seq(Seq(1))))
    ))
    val expected =
      """package com.datawizards
        |
        |/**
        |  * Generated automatically.
        |  */
        |case class NestedArray(
        |  nested2: Seq[Seq[String]],
        |  nested3: Seq[Seq[Seq[Int]]]
        |)""".stripMargin

    ClassGenerator.generateClass(df, "com.datawizards", "NestedArray").replace("\n","").replace("\r","") should equal(expected.replace("\n","").replace("\r",""))
  }

  test("Generate map") {
    val df = session.createDataFrame(Seq(
      ClassWithMap(Map(1 -> false))
    ))
    val expected =
      """package com.datawizards
        |
        |/**
        |  * Generated automatically.
        |  */
        |case class ClassWithMap(
        |  map: Map[Int, Boolean]
        |)""".stripMargin

    ClassGenerator.generateClass(df, "com.datawizards", "ClassWithMap").replace("\n","").replace("\r","") should equal(expected.replace("\n","").replace("\r",""))
  }

  test("Generate struct") {
    val df = session.createDataFrame(Seq(
      Book("b1", 2000, Person("p1", 1), Seq(Person("p2", 2)))
    ))
    val expected =
      """package com.datawizards
        |
        |/**
        |  * Generated automatically.
        |  */
        |case class Book(
        |  title: Option[String],
        |  year: Int,
        |  owner: Option[OwnerType],
        |  authors: Seq[AuthorsType]
        |)
        |case class OwnerType(
        |  name: Option[String],
        |  age: Int
        |)
        |case class AuthorsType(
        |  name: Option[String],
        |  age: Int
        |)""".stripMargin

    ClassGenerator.generateClass(df, "com.datawizards", "Book").replace("\n","").replace("\r","") should equal(expected.replace("\n","").replace("\r",""))
  }

  private def readFileContent(file: String): String =
    scala.io.Source.fromFile(file).getLines().mkString("\n")

  private def deleteDirectory(dir: String): Unit =
    new File(dir).delete()
}
