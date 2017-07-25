package com.datawizards.sparkschema2class

import java.io.{File, PrintWriter}

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._

import scala.collection.mutable.ListBuffer

object ClassGenerator {

  def generateClass(df: DataFrame, packageName: String, className: String): String = {
    s"""package $packageName
       |
       |/**
       |  * Generated automatically.
       |  */
       |case class $className(
       |  ${generateClassFields(df.schema)}
       |)
       |${generateNestedTypes(df.schema).mkString("\n")}""".stripMargin
  }

  def generateClassToDirectory(df: DataFrame, packageName: String, className: String, outputPath: String): Unit = {
    val definition = generateClass(df, packageName, className)
    val directory = outputPath + "/" + packageName.replaceAll("\\.", "/") + "/"
    new File(directory).mkdirs()
    val file = directory + className + ".scala"
    val pw = new PrintWriter(file)
    pw.write(definition)
    pw.close()
  }

  private def generateClassFields(schema: StructType): String = {
    val buffer = new ListBuffer[String]
    for (f <- schema.fields)
      buffer += generateClassField(f)
    buffer.mkString(",\n  ")
  }

  private def generateClassField(field: StructField): String = {
    val caseClassField = columnNameToField(field.name)
    s"$caseClassField: ${mapFieldTypeToScalaType(field.dataType, field.nullable, field.name)}"
  }

  private val reservedKeywords = Seq(
    "case",
    "catch",
    "class",
    "def",
    "do",
    "else",
    "extends",
    "false",
    "final",
    "for",
    "if",
    "match",
    "new",
    "null",
    "print",
    "printf",
    "println",
    "throw",
    "to",
    "trait",
    "true",
    "try",
    "until",
    "val",
    "var",
    "while",
    "with"
  )

  private def columnNameToField(columnName: String): String = {
    if (reservedKeywords.contains(columnName))
      s"""`$columnName`"""
    else
      columnName
  }

  private def mapFieldTypeToScalaType(dataType: DataType, nullable: Boolean, name: String): String = {
    val (scalaType, canBeOption) = dataType match {
      case _: BinaryType => "Array[Byte]" -> true
      case _: BooleanType => "Boolean" -> true
      case _: ByteType => "Byte" -> true
      case _: DateType => "java.sql.Date" -> true
      case _: DecimalType => "java.math.BigDecimal" -> true
      case _: DoubleType => "Double" -> true
      case _: FloatType => "Float" -> true
      case _: IntegerType => "Int" -> true
      case _: LongType => "Long" -> true
      case _: ShortType => "Short" -> true
      case _: StringType => "String" -> true
      case _: TimestampType => "java.sql.Timestamp" -> true

      case a: ArrayType => s"Seq[${mapFieldTypeToScalaType(a.elementType, false, name)}]" -> false
      case m: MapType => s"Map[${mapFieldTypeToScalaType(m.keyType, false, name)}, ${mapFieldTypeToScalaType(m.valueType, false, name)}]" -> false

      case _: StructType => generateNestedTypeName(name) -> true
    }

    if(nullable && canBeOption) "Option[" + scalaType + "]" else scalaType
  }

  private def generateNestedTypeName(fieldName: String): String = fieldName.capitalize + "Type"

  private def generateNestedTypes(schema: StructType): List[String] = {
    val buffer = new ListBuffer[String]
    for(f <- schema.fields)
       buffer ++= generateNestedTypes(f.dataType, f.name)
    buffer.toList
  }

  private def generateNestedTypes(dataType: DataType, fieldName: String): List[String] = dataType match {
    case s:StructType => generateNestedType(fieldName, s) +: generateNestedTypes(s)
    case a:ArrayType => generateNestedTypes(a.elementType, fieldName)
    case m:MapType => generateNestedTypes(m.valueType, fieldName)
    case _ => List.empty
  }

  private def generateNestedType(name: String, s: StructType): String = {
    s"""case class ${generateNestedTypeName(name)}(
       |  ${generateClassFields(s)}
       |)""".stripMargin
  }

}
