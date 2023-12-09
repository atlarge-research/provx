package lu.magalhaes.gilles.provxlib
package provenance

import provenance.storage.StorageLocation

import lu.magalhaes.gilles.provxlib.provenance.metrics.ObservationSet

trait ProvenanceGraphNode {
  val id: Int

  var storageLocation: Option[StorageLocation] = None

  def setStorageLocation(sl: StorageLocation): Unit = {
    storageLocation = Some(sl)
  }

  val metrics: ObservationSet = ObservationSet()
}
