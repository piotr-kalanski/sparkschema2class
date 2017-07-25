package com.datawizards.sparkschema2class

import java.sql.Timestamp
import java.sql.Date

object TestModel {
  case class ClassWithAllSimpleTypes(
                                      strVal: String,
                                      intVal: Int,
                                      longVal: Long,
                                      doubleVal: Double,
                                      floatVal: Float,
                                      shortVal: Short,
                                      booleanVal: Boolean,
                                      byteVal: Byte,
                                      dateVal: Date,
                                      timestampVal: Timestamp
                                    )

  case class CV(skills: Seq[String], grades: Seq[Int])
  case class NestedArray(nested2: Seq[Seq[String]], nested3: Seq[Seq[Seq[Int]]])
  case class ClassWithMap(map: Map[Int, Boolean])
  case class Person(name: String, age: Int)
  case class Book(title: String, year: Int, owner: Person, authors: Seq[Person])
}
