package lu.magalhaes.gilles.provxlib
package lineage.query

case class CaptureFilter(
    id: Int,
  dataFilter: DataPredicate[_, _] = DataPredicate(),
  provenanceFilter: ProvenancePredicate
)
