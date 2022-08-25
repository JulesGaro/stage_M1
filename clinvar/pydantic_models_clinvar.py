from pydantic import BaseModel, Field
from diagho_toolkit.models import VariantModel


class ClinvarInfosModel(BaseModel):

    AF_ESP: float = Field(default=None, description="allele frequencies from GO-ESP")
    AF_EXAC: float = Field(default=None, description="allele frequencies from ExAC")
    AF_TGP: float = Field(default=None, description="allele frequencies from TGP")
    ALLELEID: int = Field(default=None, description="the ClinVar Allele ID")
    CLNDN: str = Field(
        default=None,
        description="ClinVar's preferred disease name for the concept specified by disease identifiers in CLNDISDB",
    )
    CLNDNINCL: str = Field(
        default=None,
        description="For included Variant : ClinVar's preferred disease name for the concept specified by disease identifiers in CLNDISDB",
    )
    CLNDISDB: str = Field(
        default=None,
        description="Tag-value pairs of disease database name and identifier, e.g. OMIM:NNNNNN",
    )
    CLNDISDBINCL: str = Field(
        default=None,
        description="For included Variant: Tag-value pairs of disease database name and identifier, e.g. OMIM:NNNNNN",
    )
    CLNHGVS: str = Field(
        default=None,
        description="Top-level (primary assembly, alt, or patch) HGVS expression.",
    )
    CLNREVSTAT: str = Field(
        default=None, description="ClinVar review status for the Variation ID"
    )
    CLNSIG: str = Field(
        default=None,
        description="Clinical significance for this single variant; multiple values are separated by a vertical bar",
    )
    CLNSIGCONF: str = Field(
        default=None,
        description="Conflicting clinical significance for this single variant; multiple values are separated by a vertical bar",
    )
    CLNSIGINCL: str = Field(
        default=None,
        description="Clinical significance for a haplotype or genotype that includes this variant. Reported as pairs of VariationID:clinical significance; multiple values are separated by a vertical bar",
    )
    CLNVC: str = Field(default=None, description="Variant type")
    CLNVCSO: str = Field(
        default=None, description="Sequence Ontology id for variant type"
    )
    CLNVI: str = Field(
        default=None,
        description="the variant's clinical sources reported as tag-value pairs of database and variant identifier",
    )
    DBVARID: str = Field(
        default=None, description="nsv accessions from dbVar for the variant"
    )
    GENEINFO: str = Field(
        default=None,
        description="Gene(s) for the variant reported as gene symbol:gene id. The gene symbol and id are delimited by a colon (:) and each pair is delimited by a vertical bar (|)",
    )
    MC: str = Field(
        default=None,
        description="comma separated list of molecular consequence in the form of Sequence Ontology ID|molecular_consequence",
    )
    ORIGIN: str = Field(
        default=None,
        description="Allele origin. One or more of the following values may be added: 0 - unknown; 1 - germline; 2 - somatic; 4 - inherited; 8 - paternal; 16 - maternal; 32 - de-novo; 64 - biparental; 128 - uniparental; 256 - not-tested; 512 - tested-inconclusive; 1073741824 - other",
    )
    RS: str = Field(default=None, description="dbSNP ID (i.e. rs number)")
    SSR: int = Field(
        default=None,
        description="Variant Suspect Reason Codes. One or more of the following values may be added: 0 - unspecified, 1 - Paralog, 2 - byEST, 4 - oldAlign, 8 - Para_EST, 16 - 1kg_failed, 1024 - other",
    )


class ClinvarVariantModel(VariantModel):
    infos: ClinvarInfosModel
