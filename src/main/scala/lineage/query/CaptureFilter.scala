package lu.magalhaes.gilles.provxlib
package lineage.query

case class CaptureFilter(
    dataFilter: DataPredicate[_, _] = DataPredicate(),
    provenanceFilter: ProvenancePredicate
)
