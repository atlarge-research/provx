package lu.magalhaes.gilles.provxlib
package provenance.storage

trait StorageFormat

case class TextFile(compression: Boolean = false) extends StorageFormat
case class ObjectFile() extends StorageFormat

case class ParquetFile() extends StorageFormat
case class AvroFile() extends StorageFormat

case class ORCFile() extends StorageFormat

case class CSVFile(compression: Boolean = false) extends StorageFormat

case class JSONFormat(compression: Boolean = false) extends StorageFormat

object StorageFormat {
  def fromString(value: String): StorageFormat = {
    value match {
      case "TextFile(true)"    => TextFile(true)
      case "TextFile(false)"   => TextFile()
      case "TextFile()"        => TextFile()
      case "ObjectFile()"      => ObjectFile()
      case "ParquetFile()"     => ParquetFile()
      case "AvroFile()"        => AvroFile()
      case "ORCFile()"         => ORCFile()
      case "CSVFile(true)"     => CSVFile(true)
      case "CSVFile(false)"    => CSVFile()
      case "CSVFile()"         => CSVFile()
      case "JSONFormat(true)"  => JSONFormat(true)
      case "JSONFormat(false)" => JSONFormat()
      case "JSONFormat()"      => JSONFormat()
      case _                   => throw new UnknownError("unrecognized StorageFormat supplied")
    }
  }
}
