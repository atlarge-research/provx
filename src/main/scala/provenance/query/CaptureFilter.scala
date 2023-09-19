package lu.magalhaes.gilles.provxlib
package provenance.query

case class CaptureFilter(
    dataFilter: DataPredicate = DataPredicate(),
    provenanceFilter: ProvenancePredicate
)
