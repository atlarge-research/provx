package lu.magalhaes.gilles.provxlib
package provenance.storage

trait StorageLocation
case class HDFSLocation(path: String) extends StorageLocation
case class EmptyLocation() extends StorageLocation
