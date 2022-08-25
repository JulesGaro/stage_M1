from diagho_toolkit.models import VariantModel
from pydantic import BaseModel, Field


class GnomadInfosModel(BaseModel):
    AC: int = Field(default=None, description="Alternate allele count for samples")
    AC_afr: int = Field(
        default=None,
        description="Alternate allele count for samples of African-American ancestry",
    )
    AC_afr_female: int = Field(
        default=None,
        description="Alternate allele count for female samples of African-American ancestry",
    )
    AC_afr_male: int = Field(
        default=None,
        description="Alternate allele count for male samples of African-American ancestry",
    )
    AC_amr: int = Field(
        default=None,
        description="Alternate allele count for samples of Latino ancestry",
    )
    AC_amr_female: int = Field(
        default=None,
        description="Alternate allele count for female samples of Latino ancestry",
    )
    AC_amr_male: int = Field(
        default=None,
        description="Alternate allele count for male samples of Latino ancestry",
    )
    AC_asj: int = Field(
        default=None,
        description="Alternate allele count for samples of Ashkenazi Jewish ancestry",
    )
    AC_asj_female: int = Field(
        default=None,
        description="Alternate allele count for female samples of Ashkenazi Jewish ancestry",
    )
    AC_asj_male: int = Field(
        default=None,
        description="Alternate allele count for male samples of Ashkenazi Jewish ancestry",
    )
    AC_eas: int = Field(
        default=None,
        description="Alternate allele count for samples of East Asian ancestry",
    )
    AC_eas_female: int = Field(
        default=None,
        description="Alternate allele count for female samples of East Asian ancestry",
    )
    AC_eas_jpn: int = Field(
        default=None,
        description="Alternate allele count for samples of Japanese ancestry",
    )
    AC_eas_kor: int = Field(
        default=None,
        description="Alternate allele count for samples of Korean ancestry",
    )
    AC_eas_male: int = Field(
        default=None,
        description="Alternate allele count for male samples of East Asian ancestry",
    )
    AC_eas_oea: int = Field(
        default=None,
        description="Alternate allele count for samples of non-Korean, non-Japanese East Asian ancestry",
    )
    AC_female: int = Field(
        default=None, description="Alternate allele count for female samples"
    )
    AC_fin: int = Field(
        default=None,
        description="Alternate allele count for samples of Finnish ancestry",
    )
    AC_fin_female: int = Field(
        default=None,
        description="Alternate allele count for female samples of Finnish ancestry",
    )
    AC_fin_male: int = Field(
        default=None,
        description="Alternate allele count for male samples of Finnish ancestry",
    )
    AC_male: int = Field(
        default=None, description="Alternate allele count for male samples"
    )
    AC_nfe: int = Field(
        default=None,
        description="Alternate allele count for samples of non-Finnish European ancestry",
    )
    AC_nfe_bgr: int = Field(
        default=None,
        description="Alternate allele count for samples of Bulgarian ancestry",
    )
    AC_nfe_est: int = Field(
        default=None,
        description="Alternate allele count for samples of Estonian ancestry",
    )
    AC_nfe_female: int = Field(
        default=None,
        description="Alternate allele count for female samples of non-Finnish European ancestry",
    )
    AC_nfe_male: int = Field(
        default=None,
        description="Alternate allele count for male samples of non-Finnish European ancestry",
    )
    AC_nfe_nwe: int = Field(
        default=None,
        description="Alternate allele count for samples of North-Western European ancestry",
    )
    AC_nfe_onf: int = Field(
        default=None,
        description="Alternate allele count for samples of non-Finnish but otherwise indeterminate European ancestry",
    )
    AC_nfe_seu: int = Field(
        default=None,
        description="Alternate allele count for samples of Southern European ancestry",
    )
    AC_nfe_swe: int = Field(
        default=None,
        description="Alternate allele count for samples of Swedish ancestry",
    )
    AC_oth: int = Field(
        default=None,
        description="Alternate allele count for samples of uncertain ancestry",
    )
    AC_oth_female: int = Field(
        default=None,
        description="Alternate allele count for female samples of uncertain ancestry",
    )
    AC_oth_male: int = Field(
        default=None,
        description="Alternate allele count for male samples of uncertain ancestry",
    )
    AC_popmax: int = Field(
        default=None, description="Allele count in the population with the maximum AF"
    )
    AC_raw: int = Field(
        default=None,
        description="Alternate allele count for samples, before removing low-confidence genotypes",
    )
    AC_sas: int = Field(
        default=None,
        description="Alternate allele count for samples of South Asian ancestry",
    )
    AC_sas_female: int = Field(
        default=None,
        description="Alternate allele count for female samples of South Asian ancestry",
    )
    AC_sas_male: int = Field(
        default=None,
        description="Alternate allele count for male samples of South Asian ancestry",
    )
    AF: float = Field(default=None, description="Alternate allele frequency in samples")
    AF_afr: float = Field(
        default=None,
        description="Alternate allele frequency in samples of African-American ancestry",
    )
    AF_afr_female: float = Field(
        default=None,
        description="Alternate allele frequency in female samples of African-American ancestry",
    )
    AF_afr_male: float = Field(
        default=None,
        description="Alternate allele frequency in male samples of African-American ancestry",
    )
    AF_amr: float = Field(
        default=None,
        description="Alternate allele frequency in samples of Latino ancestry",
    )
    AF_amr_female: float = Field(
        default=None,
        description="Alternate allele frequency in female samples of Latino ancestry",
    )
    AF_amr_male: float = Field(
        default=None,
        description="Alternate allele frequency in male samples of Latino ancestry",
    )
    AF_asj: float = Field(
        default=None,
        description="Alternate allele frequency in samples of Ashkenazi Jewish ancestry",
    )
    AF_asj_female: float = Field(
        default=None,
        description="Alternate allele frequency in female samples of Ashkenazi Jewish ancestry",
    )
    AF_asj_male: float = Field(
        default=None,
        description="Alternate allele frequency in male samples of Ashkenazi Jewish ancestry",
    )
    AF_eas: float = Field(
        default=None,
        description="Alternate allele frequency in samples of East Asian ancestry",
    )
    AF_eas_female: float = Field(
        default=None,
        description="Alternate allele frequency in female samples of East Asian ancestry",
    )
    AF_eas_jpn: float = Field(
        default=None,
        description="Alternate allele frequency in samples of Japanese ancestry",
    )
    AF_eas_kor: float = Field(
        default=None,
        description="Alternate allele frequency in samples of Korean ancestry",
    )
    AF_eas_male: float = Field(
        default=None,
        description="Alternate allele frequency in male samples of East Asian ancestry",
    )
    AF_eas_oea: float = Field(
        default=None,
        description="Alternate allele frequency in samples of non-Korean, non-Japanese East Asian ancestry",
    )
    AF_female: float = Field(
        default=None, description="Alternate allele frequency in female samples"
    )
    AF_fin: float = Field(
        default=None,
        description="Alternate allele frequency in samples of Finnish ancestry",
    )
    AF_fin_female: float = Field(
        default=None,
        description="Alternate allele frequency in female samples of Finnish ancestry",
    )
    AF_fin_male: float = Field(
        default=None,
        description="Alternate allele frequency in male samples of Finnish ancestry",
    )
    AF_male: float = Field(
        default=None, description="Alternate allele frequency in male samples"
    )
    AF_nfe: float = Field(
        default=None,
        description="Alternate allele frequency in samples of non-Finnish European ancestry",
    )
    AF_nfe_bgr: float = Field(
        default=None,
        description="Alternate allele frequency in samples of Bulgarian ancestry",
    )
    AF_nfe_est: float = Field(
        default=None,
        description="Alternate allele frequency in samples of Estonian ancestry",
    )
    AF_nfe_female: float = Field(
        default=None,
        description="Alternate allele frequency in female samples of non-Finnish European ancestry",
    )
    AF_nfe_male: float = Field(
        default=None,
        description="Alternate allele frequency in male samples of non-Finnish European ancestry",
    )
    AF_nfe_nwe: float = Field(
        default=None,
        description="Alternate allele frequency in samples of North-Western European ancestry",
    )
    AF_nfe_onf: float = Field(
        default=None,
        description="Alternate allele frequency in samples of non-Finnish but otherwise indeterminate European ancestry",
    )
    AF_nfe_seu: float = Field(
        default=None,
        description="Alternate allele frequency in samples of Southern European ancestry",
    )
    AF_nfe_swe: float = Field(
        default=None,
        description="Alternate allele frequency in samples of Swedish ancestry",
    )
    AF_oth: float = Field(
        default=None,
        description="Alternate allele frequency in samples of uncertain ancestry",
    )
    AF_oth_female: float = Field(
        default=None,
        description="Alternate allele frequency in female samples of uncertain ancestry",
    )
    AF_oth_male: float = Field(
        default=None,
        description="Alternate allele frequency in male samples of uncertain ancestry",
    )
    AF_popmax: float = Field(
        default=None,
        description="Maximum allele frequency across populations (excluding samples of Ashkenazi, Finnish, and indeterminate ancestry)",
    )
    AF_raw: float = Field(
        default=None,
        description="Alternate allele frequency in samples, before removing low-confidence genotypes",
    )
    AF_sas: float = Field(
        default=None,
        description="Alternate allele frequency in samples of South Asian ancestry",
    )
    AF_sas_female: float = Field(
        default=None,
        description="Alternate allele frequency in female samples of South Asian ancestry",
    )
    AF_sas_male: float = Field(
        default=None,
        description="Alternate allele frequency in male samples of South Asian ancestry",
    )
    AN: int = Field(
        default=None, description="Total number of alleles in called genotypes"
    )
    AN_afr: int = Field(
        default=None,
        description="Total number of alleles in samples of African-American ancestry",
    )
    AN_afr_female: int = Field(
        default=None,
        description="Total number of alleles in female samples of African-American ancestry",
    )
    AN_afr_male: int = Field(
        default=None,
        description="Total number of alleles in male samples of African-American ancestry",
    )
    AN_amr: int = Field(
        default=None,
        description="Total number of alleles in samples of Latino ancestry",
    )
    AN_amr_female: int = Field(
        default=None,
        description="Total number of alleles in female samples of Latino ancestry",
    )
    AN_amr_male: int = Field(
        default=None,
        description="Total number of alleles in male samples of Latino ancestry",
    )
    AN_asj: int = Field(
        default=None,
        description="Total number of alleles in samples of Ashkenazi Jewish ancestry",
    )
    AN_asj_female: int = Field(
        default=None,
        description="Total number of alleles in female samples of Ashkenazi Jewish ancestry",
    )
    AN_asj_male: int = Field(
        default=None,
        description="Total number of alleles in male samples of Ashkenazi Jewish ancestry",
    )
    AN_eas: int = Field(
        default=None,
        description="Total number of alleles in samples of East Asian ancestry",
    )
    AN_eas_female: int = Field(
        default=None,
        description="Total number of alleles in female samples of East Asian ancestry",
    )
    AN_eas_jpn: int = Field(
        default=None,
        description="Total number of alleles in samples of Japanese ancestry",
    )
    AN_eas_kor: int = Field(
        default=None,
        description="Total number of alleles in samples of Korean ancestry",
    )
    AN_eas_male: int = Field(
        default=None,
        description="Total number of alleles in male samples of East Asian ancestry",
    )
    AN_eas_oea: int = Field(
        default=None,
        description="Total number of alleles in samples of non-Korean, non-Japanese East Asian ancestry",
    )
    AN_female: int = Field(
        default=None, description="Total number of alleles in female samples"
    )
    AN_fin: int = Field(
        default=None,
        description="Total number of alleles in samples of Finnish ancestry",
    )
    AN_fin_female: int = Field(
        default=None,
        description="Total number of alleles in female samples of Finnish ancestry",
    )
    AN_fin_male: int = Field(
        default=None,
        description="Total number of alleles in male samples of Finnish ancestry",
    )
    AN_male: int = Field(
        default=None, description="Total number of alleles in male samples"
    )
    AN_nfe: int = Field(
        default=None,
        description="Total number of alleles in samples of non-Finnish European ancestry",
    )
    AN_nfe_bgr: int = Field(
        default=None,
        description="Total number of alleles in samples of Bulgarian ancestry",
    )
    AN_nfe_est: int = Field(
        default=None,
        description="Total number of alleles in samples of Estonian ancestry",
    )
    AN_nfe_female: int = Field(
        default=None,
        description="Total number of alleles in female samples of non-Finnish European ancestry",
    )
    AN_nfe_male: int = Field(
        default=None,
        description="Total number of alleles in male samples of non-Finnish European ancestry",
    )
    AN_nfe_nwe: int = Field(
        default=None,
        description="Total number of alleles in samples of North-Western European ancestry",
    )
    AN_nfe_onf: int = Field(
        default=None,
        description="Total number of alleles in samples of non-Finnish but otherwise indeterminate European ancestry",
    )
    AN_nfe_seu: int = Field(
        default=None,
        description="Total number of alleles in samples of Southern European ancestry",
    )
    AN_nfe_swe: int = Field(
        default=None,
        description="Total number of alleles in samples of Swedish ancestry",
    )
    AN_oth: int = Field(
        default=None,
        description="Total number of alleles in samples of uncertain ancestry",
    )
    AN_oth_female: int = Field(
        default=None,
        description="Total number of alleles in female samples of uncertain ancestry",
    )
    AN_oth_male: int = Field(
        default=None,
        description="Total number of alleles in male samples of uncertain ancestry",
    )
    AN_popmax: int = Field(
        default=None,
        description="Total number of alleles in the population with the maximum AF",
    )
    AN_raw: int = Field(
        default=None,
        description="Total number of alleles in samples, before removing low-confidence genotypes",
    )
    AN_sas: int = Field(
        default=None,
        description="Total number of alleles in samples of South Asian ancestry",
    )
    AN_sas_female: int = Field(
        default=None,
        description="Total number of alleles in female samples of South Asian ancestry",
    )
    AN_sas_male: int = Field(
        default=None,
        description="Total number of alleles in male samples of South Asian ancestry",
    )
    BaseQRankSum: float = Field(
        default=None,
        description="Z-score from Wilcoxon rank sum test of alternate vs. reference base qualities",
    )
    ClippingRankSum: float = Field(
        default=None,
        description="Z-score from Wilcoxon rank sum test of alternate vs. reference number of hard clipped bases",
    )
    DP: int = Field(
        default=None,
        description="Depth of informative coverage for each sample; reads with MQ=255 or with bad mates are filtered",
    )
    FS: float = Field(
        default=None,
        description="Phred-scaled p-value of Fisher's exact test for strand bias",
    )
    InbreedingCoeff: float = Field(
        default=None,
        description="Inbreeding coefficient as estimated from the genotype likelihoods per-sample when compared against the Hardy-Weinberg expectation",
    )
    MQ: float = Field(
        default=None,
        description="Root mean square of the mapping quality of reads across all samples",
    )
    MQRankSum: float = Field(
        default=None,
        description="Z-score from Wilcoxon rank sum test of alternate vs. reference read mapping qualities",
    )
    OriginalAlleles: str = Field(
        default=None,
        description="A list of the original alleles (including REF) of the variant prior to liftover.  If the alleles were not changed during liftover, this attribute will be omitted.",
    )
    OriginalContig: str = Field(
        default=None,
        description="The name of the source contig/chromosome prior to liftover.",
    )
    OriginalStart: str = Field(
        default=None,
        description="The position of the variant on the source contig prior to liftover.",
    )
    QD: float = Field(
        default=None,
        description="Variant call confidence normalized by depth of sample reads supporting a variant",
    )
    ReadPosRankSum: float = Field(
        default=None,
        description="Z-score from Wilcoxon rank sum test of alternate vs. reference read position bias",
    )
    ReverseComplementedAlleles: str = Field(
        default=None,
        description="The REF and the ALT alleles have been reverse complemented in liftover since the mapping from the previous reference to the current one was on the negative strand.",
    )
    SOR: float = Field(
        default=None,
        description="Strand bias estimated by the symmetric odds ratio test",
    )
    SwappedAlleles: str = Field(
        default=None,
        description="The REF and the ALT alleles have been swapped in liftover due to changes in the reference. It is possible that not all INFO annotations reflect this swap, and in the genotypes, only the GT, PL, and AD fields have been modified. You should check the TAGS_TO_REVERSE parameter that was used during the LiftOver to be sure.",
    )
    VQSLOD: float = Field(
        default=None,
        description="Log-odds ratio of being a true variant versus being a false positive under the trained VQSR Gaussian mixture model",
    )
    VQSR_NEGATIVE_TRAIN_SITE: str = Field(
        default=None,
        description="Variant was used to build the negative training set of low-quality variants for VQSR",
    )
    VQSR_POSITIVE_TRAIN_SITE: str = Field(
        default=None,
        description="Variant was used to build the positive training set of high-quality variants for VQSR",
    )
    VQSR_culprit: str = Field(
        default=None,
        description="Worst-performing annotation in the VQSR Gaussian mixture model",
    )
    ab_hist_alt_bin_freq: str = Field(
        default=None,
        description="Histogram for AB in heterozygous individuals; bin edges are: 0.0|0.1|0.1|0.2|0.2|0.2|0.3|0.4|0.4|0.5|0.5|0.6|0.6|0.7|0.7|0.8|0.8|0.9|0.9|1.0|1.0",
    )
    age_hist_het_bin_freq: str = Field(
        default=None,
        description="Histogram of ages of heterozygous individuals; bin edges are: 30.0|35.0|40.0|45.0|50.0|55.0|60.0|65.0|70.0|75.0|80.0",
    )
    age_hist_het_n_larger: int = Field(
        default=None,
        description="Count of age values falling above highest histogram bin edge for heterozygous individuals",
    )
    age_hist_het_n_smaller: int = Field(
        default=None,
        description="Count of age values falling below lowest histogram bin edge for heterozygous individuals",
    )
    age_hist_hom_bin_freq: str = Field(
        default=None,
        description="Histogram of ages of homozygous alternate individuals; bin edges are: 30.0|35.0|40.0|45.0|50.0|55.0|60.0|65.0|70.0|75.0|80.0",
    )
    age_hist_hom_n_larger: int = Field(
        default=None,
        description="Count of age values falling above highest histogram bin edge for homozygous alternate individuals",
    )
    age_hist_hom_n_smaller: int = Field(
        default=None,
        description="Count of age values falling below lowest histogram bin edge for homozygous alternate individuals",
    )
    allele_type: str = Field(
        default=None, description="Allele type (snv, ins, del, or mixed)"
    )
    controls_AC: int = Field(
        default=None,
        description="Alternate allele count for samples in the controls subset",
    )
    controls_AC_afr: int = Field(
        default=None,
        description="Alternate allele count for samples of African-American ancestry in the controls subset",
    )
    controls_AC_afr_female: int = Field(
        default=None,
        description="Alternate allele count for female samples of African-American ancestry in the controls subset",
    )
    controls_AC_afr_male: int = Field(
        default=None,
        description="Alternate allele count for male samples of African-American ancestry in the controls subset",
    )
    controls_AC_amr: int = Field(
        default=None,
        description="Alternate allele count for samples of Latino ancestry in the controls subset",
    )
    controls_AC_amr_female: int = Field(
        default=None,
        description="Alternate allele count for female samples of Latino ancestry in the controls subset",
    )
    controls_AC_amr_male: int = Field(
        default=None,
        description="Alternate allele count for male samples of Latino ancestry in the controls subset",
    )
    controls_AC_asj: int = Field(
        default=None,
        description="Alternate allele count for samples of Ashkenazi Jewish ancestry in the controls subset",
    )
    controls_AC_asj_female: int = Field(
        default=None,
        description="Alternate allele count for female samples of Ashkenazi Jewish ancestry in the controls subset",
    )
    controls_AC_asj_male: int = Field(
        default=None,
        description="Alternate allele count for male samples of Ashkenazi Jewish ancestry in the controls subset",
    )
    controls_AC_eas: int = Field(
        default=None,
        description="Alternate allele count for samples of East Asian ancestry in the controls subset",
    )
    controls_AC_eas_female: int = Field(
        default=None,
        description="Alternate allele count for female samples of East Asian ancestry in the controls subset",
    )
    controls_AC_eas_jpn: int = Field(
        default=None,
        description="Alternate allele count for samples of Japanese ancestry in the controls subset",
    )
    controls_AC_eas_kor: int = Field(
        default=None,
        description="Alternate allele count for samples of Korean ancestry in the controls subset",
    )
    controls_AC_eas_male: int = Field(
        default=None,
        description="Alternate allele count for male samples of East Asian ancestry in the controls subset",
    )
    controls_AC_eas_oea: int = Field(
        default=None,
        description="Alternate allele count for samples of non-Korean, non-Japanese East Asian ancestry in the controls subset",
    )
    controls_AC_female: int = Field(
        default=None,
        description="Alternate allele count for female samples in the controls subset",
    )
    controls_AC_fin: int = Field(
        default=None,
        description="Alternate allele count for samples of Finnish ancestry in the controls subset",
    )
    controls_AC_fin_female: int = Field(
        default=None,
        description="Alternate allele count for female samples of Finnish ancestry in the controls subset",
    )
    controls_AC_fin_male: int = Field(
        default=None,
        description="Alternate allele count for male samples of Finnish ancestry in the controls subset",
    )
    controls_AC_male: int = Field(
        default=None,
        description="Alternate allele count for male samples in the controls subset",
    )
    controls_AC_nfe: int = Field(
        default=None,
        description="Alternate allele count for samples of non-Finnish European ancestry in the controls subset",
    )
    controls_AC_nfe_bgr: int = Field(
        default=None,
        description="Alternate allele count for samples of Bulgarian ancestry in the controls subset",
    )
    controls_AC_nfe_est: int = Field(
        default=None,
        description="Alternate allele count for samples of Estonian ancestry in the controls subset",
    )
    controls_AC_nfe_female: int = Field(
        default=None,
        description="Alternate allele count for female samples of non-Finnish European ancestry in the controls subset",
    )
    controls_AC_nfe_male: int = Field(
        default=None,
        description="Alternate allele count for male samples of non-Finnish European ancestry in the controls subset",
    )
    controls_AC_nfe_nwe: int = Field(
        default=None,
        description="Alternate allele count for samples of North-Western European ancestry in the controls subset",
    )
    controls_AC_nfe_onf: int = Field(
        default=None,
        description="Alternate allele count for samples of non-Finnish but otherwise indeterminate European ancestry in the controls subset",
    )
    controls_AC_nfe_seu: int = Field(
        default=None,
        description="Alternate allele count for samples of Southern European ancestry in the controls subset",
    )
    controls_AC_nfe_swe: int = Field(
        default=None,
        description="Alternate allele count for samples of Swedish ancestry in the controls subset",
    )
    controls_AC_oth: int = Field(
        default=None,
        description="Alternate allele count for samples of uncertain ancestry in the controls subset",
    )
    controls_AC_oth_female: int = Field(
        default=None,
        description="Alternate allele count for female samples of uncertain ancestry in the controls subset",
    )
    controls_AC_oth_male: int = Field(
        default=None,
        description="Alternate allele count for male samples of uncertain ancestry in the controls subset",
    )
    controls_AC_popmax: int = Field(
        default=None,
        description="Allele count in the population with the maximum AF in the controls subset",
    )
    controls_AC_raw: int = Field(
        default=None,
        description="Alternate allele count for samples in the controls subset, before removing low-confidence genotypes",
    )
    controls_AC_sas: int = Field(
        default=None,
        description="Alternate allele count for samples of South Asian ancestry in the controls subset",
    )
    controls_AC_sas_female: int = Field(
        default=None,
        description="Alternate allele count for female samples of South Asian ancestry in the controls subset",
    )
    controls_AC_sas_male: int = Field(
        default=None,
        description="Alternate allele count for male samples of South Asian ancestry in the controls subset",
    )
    controls_AF: float = Field(
        default=None,
        description="Alternate allele frequency in samples in the controls subset",
    )
    controls_AF_afr: float = Field(
        default=None,
        description="Alternate allele frequency in samples of African-American ancestry in the controls subset",
    )
    controls_AF_afr_female: float = Field(
        default=None,
        description="Alternate allele frequency in female samples of African-American ancestry in the controls subset",
    )
    controls_AF_afr_male: float = Field(
        default=None,
        description="Alternate allele frequency in male samples of African-American ancestry in the controls subset",
    )
    controls_AF_amr: float = Field(
        default=None,
        description="Alternate allele frequency in samples of Latino ancestry in the controls subset",
    )
    controls_AF_amr_female: float = Field(
        default=None,
        description="Alternate allele frequency in female samples of Latino ancestry in the controls subset",
    )
    controls_AF_amr_male: float = Field(
        default=None,
        description="Alternate allele frequency in male samples of Latino ancestry in the controls subset",
    )
    controls_AF_asj: float = Field(
        default=None,
        description="Alternate allele frequency in samples of Ashkenazi Jewish ancestry in the controls subset",
    )
    controls_AF_asj_female: float = Field(
        default=None,
        description="Alternate allele frequency in female samples of Ashkenazi Jewish ancestry in the controls subset",
    )
    controls_AF_asj_male: float = Field(
        default=None,
        description="Alternate allele frequency in male samples of Ashkenazi Jewish ancestry in the controls subset",
    )
    controls_AF_eas: float = Field(
        default=None,
        description="Alternate allele frequency in samples of East Asian ancestry in the controls subset",
    )
    controls_AF_eas_female: float = Field(
        default=None,
        description="Alternate allele frequency in female samples of East Asian ancestry in the controls subset",
    )
    controls_AF_eas_jpn: float = Field(
        default=None,
        description="Alternate allele frequency in samples of Japanese ancestry in the controls subset",
    )
    controls_AF_eas_kor: float = Field(
        default=None,
        description="Alternate allele frequency in samples of Korean ancestry in the controls subset",
    )
    controls_AF_eas_male: float = Field(
        default=None,
        description="Alternate allele frequency in male samples of East Asian ancestry in the controls subset",
    )
    controls_AF_eas_oea: float = Field(
        default=None,
        description="Alternate allele frequency in samples of non-Korean, non-Japanese East Asian ancestry in the controls subset",
    )
    controls_AF_female: float = Field(
        default=None,
        description="Alternate allele frequency in female samples in the controls subset",
    )
    controls_AF_fin: float = Field(
        default=None,
        description="Alternate allele frequency in samples of Finnish ancestry in the controls subset",
    )
    controls_AF_fin_female: float = Field(
        default=None,
        description="Alternate allele frequency in female samples of Finnish ancestry in the controls subset",
    )
    controls_AF_fin_male: float = Field(
        default=None,
        description="Alternate allele frequency in male samples of Finnish ancestry in the controls subset",
    )
    controls_AF_male: float = Field(
        default=None,
        description="Alternate allele frequency in male samples in the controls subset",
    )
    controls_AF_nfe: float = Field(
        default=None,
        description="Alternate allele frequency in samples of non-Finnish European ancestry in the controls subset",
    )
    controls_AF_nfe_bgr: float = Field(
        default=None,
        description="Alternate allele frequency in samples of Bulgarian ancestry in the controls subset",
    )
    controls_AF_nfe_est: float = Field(
        default=None,
        description="Alternate allele frequency in samples of Estonian ancestry in the controls subset",
    )
    controls_AF_nfe_female: float = Field(
        default=None,
        description="Alternate allele frequency in female samples of non-Finnish European ancestry in the controls subset",
    )
    controls_AF_nfe_male: float = Field(
        default=None,
        description="Alternate allele frequency in male samples of non-Finnish European ancestry in the controls subset",
    )
    controls_AF_nfe_nwe: float = Field(
        default=None,
        description="Alternate allele frequency in samples of North-Western European ancestry in the controls subset",
    )
    controls_AF_nfe_onf: float = Field(
        default=None,
        description="Alternate allele frequency in samples of non-Finnish but otherwise indeterminate European ancestry in the controls subset",
    )
    controls_AF_nfe_seu: float = Field(
        default=None,
        description="Alternate allele frequency in samples of Southern European ancestry in the controls subset",
    )
    controls_AF_nfe_swe: float = Field(
        default=None,
        description="Alternate allele frequency in samples of Swedish ancestry in the controls subset",
    )
    controls_AF_oth: float = Field(
        default=None,
        description="Alternate allele frequency in samples of uncertain ancestry in the controls subset",
    )
    controls_AF_oth_female: float = Field(
        default=None,
        description="Alternate allele frequency in female samples of uncertain ancestry in the controls subset",
    )
    controls_AF_oth_male: float = Field(
        default=None,
        description="Alternate allele frequency in male samples of uncertain ancestry in the controls subset",
    )
    controls_AF_popmax: float = Field(
        default=None,
        description="Maximum allele frequency across populations (excluding samples of Ashkenazi, Finnish, and indeterminate ancestry) in the controls subset",
    )
    controls_AF_raw: float = Field(
        default=None,
        description="Alternate allele frequency in samples in the controls subset, before removing low-confidence genotypes",
    )
    controls_AF_sas: float = Field(
        default=None,
        description="Alternate allele frequency in samples of South Asian ancestry in the controls subset",
    )
    controls_AF_sas_female: float = Field(
        default=None,
        description="Alternate allele frequency in female samples of South Asian ancestry in the controls subset",
    )
    controls_AF_sas_male: float = Field(
        default=None,
        description="Alternate allele frequency in male samples of South Asian ancestry in the controls subset",
    )
    controls_AN: int = Field(
        default=None,
        description="Total number of alleles in samples in the controls subset",
    )
    controls_AN_afr: int = Field(
        default=None,
        description="Total number of alleles in samples of African-American ancestry in the controls subset",
    )
    controls_AN_afr_female: int = Field(
        default=None,
        description="Total number of alleles in female samples of African-American ancestry in the controls subset",
    )
    controls_AN_afr_male: int = Field(
        default=None,
        description="Total number of alleles in male samples of African-American ancestry in the controls subset",
    )
    controls_AN_amr: int = Field(
        default=None,
        description="Total number of alleles in samples of Latino ancestry in the controls subset",
    )
    controls_AN_amr_female: int = Field(
        default=None,
        description="Total number of alleles in female samples of Latino ancestry in the controls subset",
    )
    controls_AN_amr_male: int = Field(
        default=None,
        description="Total number of alleles in male samples of Latino ancestry in the controls subset",
    )
    controls_AN_asj: int = Field(
        default=None,
        description="Total number of alleles in samples of Ashkenazi Jewish ancestry in the controls subset",
    )
    controls_AN_asj_female: int = Field(
        default=None,
        description="Total number of alleles in female samples of Ashkenazi Jewish ancestry in the controls subset",
    )
    controls_AN_asj_male: int = Field(
        default=None,
        description="Total number of alleles in male samples of Ashkenazi Jewish ancestry in the controls subset",
    )
    controls_AN_eas: int = Field(
        default=None,
        description="Total number of alleles in samples of East Asian ancestry in the controls subset",
    )
    controls_AN_eas_female: int = Field(
        default=None,
        description="Total number of alleles in female samples of East Asian ancestry in the controls subset",
    )
    controls_AN_eas_jpn: int = Field(
        default=None,
        description="Total number of alleles in samples of Japanese ancestry in the controls subset",
    )
    controls_AN_eas_kor: int = Field(
        default=None,
        description="Total number of alleles in samples of Korean ancestry in the controls subset",
    )
    controls_AN_eas_male: int = Field(
        default=None,
        description="Total number of alleles in male samples of East Asian ancestry in the controls subset",
    )
    controls_AN_eas_oea: int = Field(
        default=None,
        description="Total number of alleles in samples of non-Korean, non-Japanese East Asian ancestry in the controls subset",
    )
    controls_AN_female: int = Field(
        default=None,
        description="Total number of alleles in female samples in the controls subset",
    )
    controls_AN_fin: int = Field(
        default=None,
        description="Total number of alleles in samples of Finnish ancestry in the controls subset",
    )
    controls_AN_fin_female: int = Field(
        default=None,
        description="Total number of alleles in female samples of Finnish ancestry in the controls subset",
    )
    controls_AN_fin_male: int = Field(
        default=None,
        description="Total number of alleles in male samples of Finnish ancestry in the controls subset",
    )
    controls_AN_male: int = Field(
        default=None,
        description="Total number of alleles in male samples in the controls subset",
    )
    controls_AN_nfe: int = Field(
        default=None,
        description="Total number of alleles in samples of non-Finnish European ancestry in the controls subset",
    )
    controls_AN_nfe_bgr: int = Field(
        default=None,
        description="Total number of alleles in samples of Bulgarian ancestry in the controls subset",
    )
    controls_AN_nfe_est: int = Field(
        default=None,
        description="Total number of alleles in samples of Estonian ancestry in the controls subset",
    )
    controls_AN_nfe_female: int = Field(
        default=None,
        description="Total number of alleles in female samples of non-Finnish European ancestry in the controls subset",
    )
    controls_AN_nfe_male: int = Field(
        default=None,
        description="Total number of alleles in male samples of non-Finnish European ancestry in the controls subset",
    )
    controls_AN_nfe_nwe: int = Field(
        default=None,
        description="Total number of alleles in samples of North-Western European ancestry in the controls subset",
    )
    controls_AN_nfe_onf: int = Field(
        default=None,
        description="Total number of alleles in samples of non-Finnish but otherwise indeterminate European ancestry in the controls subset",
    )
    controls_AN_nfe_seu: int = Field(
        default=None,
        description="Total number of alleles in samples of Southern European ancestry in the controls subset",
    )
    controls_AN_nfe_swe: int = Field(
        default=None,
        description="Total number of alleles in samples of Swedish ancestry in the controls subset",
    )
    controls_AN_oth: int = Field(
        default=None,
        description="Total number of alleles in samples of uncertain ancestry in the controls subset",
    )
    controls_AN_oth_female: int = Field(
        default=None,
        description="Total number of alleles in female samples of uncertain ancestry in the controls subset",
    )
    controls_AN_oth_male: int = Field(
        default=None,
        description="Total number of alleles in male samples of uncertain ancestry in the controls subset",
    )
    controls_AN_popmax: int = Field(
        default=None,
        description="Total number of alleles in the population with the maximum AF in the controls subset",
    )
    controls_AN_raw: int = Field(
        default=None,
        description="Total number of alleles in samples in the controls subset, before removing low-confidence genotypes",
    )
    controls_AN_sas: int = Field(
        default=None,
        description="Total number of alleles in samples of South Asian ancestry in the controls subset",
    )
    controls_AN_sas_female: int = Field(
        default=None,
        description="Total number of alleles in female samples of South Asian ancestry in the controls subset",
    )
    controls_AN_sas_male: int = Field(
        default=None,
        description="Total number of alleles in male samples of South Asian ancestry in the controls subset",
    )
    controls_faf95: float = Field(
        default=None,
        description="Filtering allele frequency (using Poisson 95% CI) for samples in the controls subset",
    )
    controls_faf95_afr: float = Field(
        default=None,
        description="Filtering allele frequency (using Poisson 95% CI) for samples of African-American ancestry in the controls subset",
    )
    controls_faf95_amr: float = Field(
        default=None,
        description="Filtering allele frequency (using Poisson 95% CI) for samples of Latino ancestry in the controls subset",
    )
    controls_faf95_eas: float = Field(
        default=None,
        description="Filtering allele frequency (using Poisson 95% CI) for samples of East Asian ancestry in the controls subset",
    )
    controls_faf95_nfe: float = Field(
        default=None,
        description="Filtering allele frequency (using Poisson 95% CI) for samples of non-Finnish European ancestry in the controls subset",
    )
    controls_faf95_sas: float = Field(
        default=None,
        description="Filtering allele frequency (using Poisson 95% CI) for samples of South Asian ancestry in the controls subset",
    )
    controls_faf99: float = Field(
        default=None,
        description="Filtering allele frequency (using Poisson 99% CI) for samples in the controls subset",
    )
    controls_faf99_afr: float = Field(
        default=None,
        description="Filtering allele frequency (using Poisson 99% CI) for samples of African-American ancestry in the controls subset",
    )
    controls_faf99_amr: float = Field(
        default=None,
        description="Filtering allele frequency (using Poisson 99% CI) for samples of Latino ancestry in the controls subset",
    )
    controls_faf99_eas: float = Field(
        default=None,
        description="Filtering allele frequency (using Poisson 99% CI) for samples of East Asian ancestry in the controls subset",
    )
    controls_faf99_nfe: float = Field(
        default=None,
        description="Filtering allele frequency (using Poisson 99% CI) for samples of non-Finnish European ancestry in the controls subset",
    )
    controls_faf99_sas: float = Field(
        default=None,
        description="Filtering allele frequency (using Poisson 99% CI) for samples of South Asian ancestry in the controls subset",
    )
    controls_nhomalt: int = Field(
        default=None,
        description="Count of homozygous individuals in samples in the controls subset",
    )
    controls_nhomalt_afr: int = Field(
        default=None,
        description="Count of homozygous individuals in samples of African-American ancestry in the controls subset",
    )
    controls_nhomalt_afr_female: int = Field(
        default=None,
        description="Count of homozygous individuals in female samples of African-American ancestry in the controls subset",
    )
    controls_nhomalt_afr_male: int = Field(
        default=None,
        description="Count of homozygous individuals in male samples of African-American ancestry in the controls subset",
    )
    controls_nhomalt_amr: int = Field(
        default=None,
        description="Count of homozygous individuals in samples of Latino ancestry in the controls subset",
    )
    controls_nhomalt_amr_female: int = Field(
        default=None,
        description="Count of homozygous individuals in female samples of Latino ancestry in the controls subset",
    )
    controls_nhomalt_amr_male: int = Field(
        default=None,
        description="Count of homozygous individuals in male samples of Latino ancestry in the controls subset",
    )
    controls_nhomalt_asj: int = Field(
        default=None,
        description="Count of homozygous individuals in samples of Ashkenazi Jewish ancestry in the controls subset",
    )
    controls_nhomalt_asj_female: int = Field(
        default=None,
        description="Count of homozygous individuals in female samples of Ashkenazi Jewish ancestry in the controls subset",
    )
    controls_nhomalt_asj_male: int = Field(
        default=None,
        description="Count of homozygous individuals in male samples of Ashkenazi Jewish ancestry in the controls subset",
    )
    controls_nhomalt_eas: int = Field(
        default=None,
        description="Count of homozygous individuals in samples of East Asian ancestry in the controls subset",
    )
    controls_nhomalt_eas_female: int = Field(
        default=None,
        description="Count of homozygous individuals in female samples of East Asian ancestry in the controls subset",
    )
    controls_nhomalt_eas_jpn: int = Field(
        default=None,
        description="Count of homozygous individuals in samples of Japanese ancestry in the controls subset",
    )
    controls_nhomalt_eas_kor: int = Field(
        default=None,
        description="Count of homozygous individuals in samples of Korean ancestry in the controls subset",
    )
    controls_nhomalt_eas_male: int = Field(
        default=None,
        description="Count of homozygous individuals in male samples of East Asian ancestry in the controls subset",
    )
    controls_nhomalt_eas_oea: int = Field(
        default=None,
        description="Count of homozygous individuals in samples of non-Korean, non-Japanese East Asian ancestry in the controls subset",
    )
    controls_nhomalt_female: int = Field(
        default=None,
        description="Count of homozygous individuals in female samples in the controls subset",
    )
    controls_nhomalt_fin: int = Field(
        default=None,
        description="Count of homozygous individuals in samples of Finnish ancestry in the controls subset",
    )
    controls_nhomalt_fin_female: int = Field(
        default=None,
        description="Count of homozygous individuals in female samples of Finnish ancestry in the controls subset",
    )
    controls_nhomalt_fin_male: int = Field(
        default=None,
        description="Count of homozygous individuals in male samples of Finnish ancestry in the controls subset",
    )
    controls_nhomalt_male: int = Field(
        default=None,
        description="Count of homozygous individuals in male samples in the controls subset",
    )
    controls_nhomalt_nfe: int = Field(
        default=None,
        description="Count of homozygous individuals in samples of non-Finnish European ancestry in the controls subset",
    )
    controls_nhomalt_nfe_bgr: int = Field(
        default=None,
        description="Count of homozygous individuals in samples of Bulgarian ancestry in the controls subset",
    )
    controls_nhomalt_nfe_est: int = Field(
        default=None,
        description="Count of homozygous individuals in samples of Estonian ancestry in the controls subset",
    )
    controls_nhomalt_nfe_female: int = Field(
        default=None,
        description="Count of homozygous individuals in female samples of non-Finnish European ancestry in the controls subset",
    )
    controls_nhomalt_nfe_male: int = Field(
        default=None,
        description="Count of homozygous individuals in male samples of non-Finnish European ancestry in the controls subset",
    )
    controls_nhomalt_nfe_nwe: int = Field(
        default=None,
        description="Count of homozygous individuals in samples of North-Western European ancestry in the controls subset",
    )
    controls_nhomalt_nfe_onf: int = Field(
        default=None,
        description="Count of homozygous individuals in samples of non-Finnish but otherwise indeterminate European ancestry in the controls subset",
    )
    controls_nhomalt_nfe_seu: int = Field(
        default=None,
        description="Count of homozygous individuals in samples of Southern European ancestry in the controls subset",
    )
    controls_nhomalt_nfe_swe: int = Field(
        default=None,
        description="Count of homozygous individuals in samples of Swedish ancestry in the controls subset",
    )
    controls_nhomalt_oth: int = Field(
        default=None,
        description="Count of homozygous individuals in samples of uncertain ancestry in the controls subset",
    )
    controls_nhomalt_oth_female: int = Field(
        default=None,
        description="Count of homozygous individuals in female samples of uncertain ancestry in the controls subset",
    )
    controls_nhomalt_oth_male: int = Field(
        default=None,
        description="Count of homozygous individuals in male samples of uncertain ancestry in the controls subset",
    )
    controls_nhomalt_popmax: int = Field(
        default=None,
        description="Count of homozygous individuals in the population with the maximum allele frequency in the controls subset",
    )
    controls_nhomalt_raw: int = Field(
        default=None,
        description="Count of homozygous individuals in samples in the controls subset, before removing low-confidence genotypes",
    )
    controls_nhomalt_sas: int = Field(
        default=None,
        description="Count of homozygous individuals in samples of South Asian ancestry in the controls subset",
    )
    controls_nhomalt_sas_female: int = Field(
        default=None,
        description="Count of homozygous individuals in female samples of South Asian ancestry in the controls subset",
    )
    controls_nhomalt_sas_male: int = Field(
        default=None,
        description="Count of homozygous individuals in male samples of South Asian ancestry in the controls subset",
    )
    controls_popmax: str = Field(
        default=None, description="Population with maximum AF in the controls subset"
    )
    decoy: str = Field(
        default=None, description="Variant falls within a reference decoy region"
    )
    dp_hist_all_bin_freq: str = Field(
        default=None,
        description="Histogram for DP; bin edges are: 0|5|10|15|20|25|30|35|40|45|50|55|60|65|70|75|80|85|90|95|100",
    )
    dp_hist_all_n_larger: int = Field(
        default=None,
        description="Count of DP values falling above highest histogram bin edge",
    )
    dp_hist_alt_bin_freq: str = Field(
        default=None,
        description="Histogram for DP in heterozygous individuals; bin edges are: 0|5|10|15|20|25|30|35|40|45|50|55|60|65|70|75|80|85|90|95|100",
    )
    dp_hist_alt_n_larger: int = Field(
        default=None,
        description="Count of DP values falling above highest histogram bin edge",
    )
    faf95: float = Field(
        default=None,
        description="Filtering allele frequency (using Poisson 95% CI) for samples",
    )
    faf95_afr: float = Field(
        default=None,
        description="Filtering allele frequency (using Poisson 95% CI) for samples of African-American ancestry",
    )
    faf95_amr: float = Field(
        default=None,
        description="Filtering allele frequency (using Poisson 95% CI) for samples of Latino ancestry",
    )
    faf95_eas: float = Field(
        default=None,
        description="Filtering allele frequency (using Poisson 95% CI) for samples of East Asian ancestry",
    )
    faf95_nfe: float = Field(
        default=None,
        description="Filtering allele frequency (using Poisson 95% CI) for samples of non-Finnish European ancestry",
    )
    faf95_sas: float = Field(
        default=None,
        description="Filtering allele frequency (using Poisson 95% CI) for samples of South Asian ancestry",
    )
    faf99: float = Field(
        default=None,
        description="Filtering allele frequency (using Poisson 99% CI) for samples",
    )
    faf99_afr: float = Field(
        default=None,
        description="Filtering allele frequency (using Poisson 99% CI) for samples of African-American ancestry",
    )
    faf99_amr: float = Field(
        default=None,
        description="Filtering allele frequency (using Poisson 99% CI) for samples of Latino ancestry",
    )
    faf99_eas: float = Field(
        default=None,
        description="Filtering allele frequency (using Poisson 99% CI) for samples of East Asian ancestry",
    )
    faf99_nfe: float = Field(
        default=None,
        description="Filtering allele frequency (using Poisson 99% CI) for samples of non-Finnish European ancestry",
    )
    faf99_sas: float = Field(
        default=None,
        description="Filtering allele frequency (using Poisson 99% CI) for samples of South Asian ancestry",
    )
    gq_hist_all_bin_freq: str = Field(
        default=None,
        description="Histogram for GQ; bin edges are: 0|5|10|15|20|25|30|35|40|45|50|55|60|65|70|75|80|85|90|95|100",
    )
    gq_hist_alt_bin_freq: str = Field(
        default=None,
        description="Histogram for GQ in heterozygous individuals; bin edges are: 0|5|10|15|20|25|30|35|40|45|50|55|60|65|70|75|80|85|90|95|100",
    )
    has_star: str = Field(
        default=None,
        description="Variant locus coincides with a spanning deletion (represented by a star) observed elsewhere in the callset",
    )
    lcr: str = Field(
        default=None, description="Variant falls within a low complexity region"
    )
    n_alt_alleles: int = Field(
        default=None,
        description="Total number of alternate alleles observed at variant locus",
    )
    nhomalt: int = Field(
        default=None, description="Count of homozygous individuals in samples"
    )
    nhomalt_afr: int = Field(
        default=None,
        description="Count of homozygous individuals in samples of African-American ancestry",
    )
    nhomalt_afr_female: int = Field(
        default=None,
        description="Count of homozygous individuals in female samples of African-American ancestry",
    )
    nhomalt_afr_male: int = Field(
        default=None,
        description="Count of homozygous individuals in male samples of African-American ancestry",
    )
    nhomalt_amr: int = Field(
        default=None,
        description="Count of homozygous individuals in samples of Latino ancestry",
    )
    nhomalt_amr_female: int = Field(
        default=None,
        description="Count of homozygous individuals in female samples of Latino ancestry",
    )
    nhomalt_amr_male: int = Field(
        default=None,
        description="Count of homozygous individuals in male samples of Latino ancestry",
    )
    nhomalt_asj: int = Field(
        default=None,
        description="Count of homozygous individuals in samples of Ashkenazi Jewish ancestry",
    )
    nhomalt_asj_female: int = Field(
        default=None,
        description="Count of homozygous individuals in female samples of Ashkenazi Jewish ancestry",
    )
    nhomalt_asj_male: int = Field(
        default=None,
        description="Count of homozygous individuals in male samples of Ashkenazi Jewish ancestry",
    )
    nhomalt_eas: int = Field(
        default=None,
        description="Count of homozygous individuals in samples of East Asian ancestry",
    )
    nhomalt_eas_female: int = Field(
        default=None,
        description="Count of homozygous individuals in female samples of East Asian ancestry",
    )
    nhomalt_eas_jpn: int = Field(
        default=None,
        description="Count of homozygous individuals in samples of Japanese ancestry",
    )
    nhomalt_eas_kor: int = Field(
        default=None,
        description="Count of homozygous individuals in samples of Korean ancestry",
    )
    nhomalt_eas_male: int = Field(
        default=None,
        description="Count of homozygous individuals in male samples of East Asian ancestry",
    )
    nhomalt_eas_oea: int = Field(
        default=None,
        description="Count of homozygous individuals in samples of non-Korean, non-Japanese East Asian ancestry",
    )
    nhomalt_female: int = Field(
        default=None, description="Count of homozygous individuals in female samples"
    )
    nhomalt_fin: int = Field(
        default=None,
        description="Count of homozygous individuals in samples of Finnish ancestry",
    )
    nhomalt_fin_female: int = Field(
        default=None,
        description="Count of homozygous individuals in female samples of Finnish ancestry",
    )
    nhomalt_fin_male: int = Field(
        default=None,
        description="Count of homozygous individuals in male samples of Finnish ancestry",
    )
    nhomalt_male: int = Field(
        default=None, description="Count of homozygous individuals in male samples"
    )
    nhomalt_nfe: int = Field(
        default=None,
        description="Count of homozygous individuals in samples of non-Finnish European ancestry",
    )
    nhomalt_nfe_bgr: int = Field(
        default=None,
        description="Count of homozygous individuals in samples of Bulgarian ancestry",
    )
    nhomalt_nfe_est: int = Field(
        default=None,
        description="Count of homozygous individuals in samples of Estonian ancestry",
    )
    nhomalt_nfe_female: int = Field(
        default=None,
        description="Count of homozygous individuals in female samples of non-Finnish European ancestry",
    )
    nhomalt_nfe_male: int = Field(
        default=None,
        description="Count of homozygous individuals in male samples of non-Finnish European ancestry",
    )
    nhomalt_nfe_nwe: int = Field(
        default=None,
        description="Count of homozygous individuals in samples of North-Western European ancestry",
    )
    nhomalt_nfe_onf: int = Field(
        default=None,
        description="Count of homozygous individuals in samples of non-Finnish but otherwise indeterminate European ancestry",
    )
    nhomalt_nfe_seu: int = Field(
        default=None,
        description="Count of homozygous individuals in samples of Southern European ancestry",
    )
    nhomalt_nfe_swe: int = Field(
        default=None,
        description="Count of homozygous individuals in samples of Swedish ancestry",
    )
    nhomalt_oth: int = Field(
        default=None,
        description="Count of homozygous individuals in samples of uncertain ancestry",
    )
    nhomalt_oth_female: int = Field(
        default=None,
        description="Count of homozygous individuals in female samples of uncertain ancestry",
    )
    nhomalt_oth_male: int = Field(
        default=None,
        description="Count of homozygous individuals in male samples of uncertain ancestry",
    )
    nhomalt_popmax: int = Field(
        default=None,
        description="Count of homozygous individuals in the population with the maximum allele frequency",
    )
    nhomalt_raw: int = Field(
        default=None,
        description="Count of homozygous individuals in samples, before removing low-confidence genotypes",
    )
    nhomalt_sas: int = Field(
        default=None,
        description="Count of homozygous individuals in samples of South Asian ancestry",
    )
    nhomalt_sas_female: int = Field(
        default=None,
        description="Count of homozygous individuals in female samples of South Asian ancestry",
    )
    nhomalt_sas_male: int = Field(
        default=None,
        description="Count of homozygous individuals in male samples of South Asian ancestry",
    )
    non_cancer_AC: int = Field(
        default=None,
        description="Alternate allele count for samples in the non_cancer subset",
    )
    non_cancer_AC_afr: int = Field(
        default=None,
        description="Alternate allele count for samples of African-American ancestry in the non_cancer subset",
    )
    non_cancer_AC_afr_female: int = Field(
        default=None,
        description="Alternate allele count for female samples of African-American ancestry in the non_cancer subset",
    )
    non_cancer_AC_afr_male: int = Field(
        default=None,
        description="Alternate allele count for male samples of African-American ancestry in the non_cancer subset",
    )
    non_cancer_AC_amr: int = Field(
        default=None,
        description="Alternate allele count for samples of Latino ancestry in the non_cancer subset",
    )
    non_cancer_AC_amr_female: int = Field(
        default=None,
        description="Alternate allele count for female samples of Latino ancestry in the non_cancer subset",
    )
    non_cancer_AC_amr_male: int = Field(
        default=None,
        description="Alternate allele count for male samples of Latino ancestry in the non_cancer subset",
    )
    non_cancer_AC_asj: int = Field(
        default=None,
        description="Alternate allele count for samples of Ashkenazi Jewish ancestry in the non_cancer subset",
    )
    non_cancer_AC_asj_female: int = Field(
        default=None,
        description="Alternate allele count for female samples of Ashkenazi Jewish ancestry in the non_cancer subset",
    )
    non_cancer_AC_asj_male: int = Field(
        default=None,
        description="Alternate allele count for male samples of Ashkenazi Jewish ancestry in the non_cancer subset",
    )
    non_cancer_AC_eas: int = Field(
        default=None,
        description="Alternate allele count for samples of East Asian ancestry in the non_cancer subset",
    )
    non_cancer_AC_eas_female: int = Field(
        default=None,
        description="Alternate allele count for female samples of East Asian ancestry in the non_cancer subset",
    )
    non_cancer_AC_eas_jpn: int = Field(
        default=None,
        description="Alternate allele count for samples of Japanese ancestry in the non_cancer subset",
    )
    non_cancer_AC_eas_kor: int = Field(
        default=None,
        description="Alternate allele count for samples of Korean ancestry in the non_cancer subset",
    )
    non_cancer_AC_eas_male: int = Field(
        default=None,
        description="Alternate allele count for male samples of East Asian ancestry in the non_cancer subset",
    )
    non_cancer_AC_eas_oea: int = Field(
        default=None,
        description="Alternate allele count for samples of non-Korean, non-Japanese East Asian ancestry in the non_cancer subset",
    )
    non_cancer_AC_female: int = Field(
        default=None,
        description="Alternate allele count for female samples in the non_cancer subset",
    )
    non_cancer_AC_fin: int = Field(
        default=None,
        description="Alternate allele count for samples of Finnish ancestry in the non_cancer subset",
    )
    non_cancer_AC_fin_female: int = Field(
        default=None,
        description="Alternate allele count for female samples of Finnish ancestry in the non_cancer subset",
    )
    non_cancer_AC_fin_male: int = Field(
        default=None,
        description="Alternate allele count for male samples of Finnish ancestry in the non_cancer subset",
    )
    non_cancer_AC_male: int = Field(
        default=None,
        description="Alternate allele count for male samples in the non_cancer subset",
    )
    non_cancer_AC_nfe: int = Field(
        default=None,
        description="Alternate allele count for samples of non-Finnish European ancestry in the non_cancer subset",
    )
    non_cancer_AC_nfe_bgr: int = Field(
        default=None,
        description="Alternate allele count for samples of Bulgarian ancestry in the non_cancer subset",
    )
    non_cancer_AC_nfe_est: int = Field(
        default=None,
        description="Alternate allele count for samples of Estonian ancestry in the non_cancer subset",
    )
    non_cancer_AC_nfe_female: int = Field(
        default=None,
        description="Alternate allele count for female samples of non-Finnish European ancestry in the non_cancer subset",
    )
    non_cancer_AC_nfe_male: int = Field(
        default=None,
        description="Alternate allele count for male samples of non-Finnish European ancestry in the non_cancer subset",
    )
    non_cancer_AC_nfe_nwe: int = Field(
        default=None,
        description="Alternate allele count for samples of North-Western European ancestry in the non_cancer subset",
    )
    non_cancer_AC_nfe_onf: int = Field(
        default=None,
        description="Alternate allele count for samples of non-Finnish but otherwise indeterminate European ancestry in the non_cancer subset",
    )
    non_cancer_AC_nfe_seu: int = Field(
        default=None,
        description="Alternate allele count for samples of Southern European ancestry in the non_cancer subset",
    )
    non_cancer_AC_nfe_swe: int = Field(
        default=None,
        description="Alternate allele count for samples of Swedish ancestry in the non_cancer subset",
    )
    non_cancer_AC_oth: int = Field(
        default=None,
        description="Alternate allele count for samples of uncertain ancestry in the non_cancer subset",
    )
    non_cancer_AC_oth_female: int = Field(
        default=None,
        description="Alternate allele count for female samples of uncertain ancestry in the non_cancer subset",
    )
    non_cancer_AC_oth_male: int = Field(
        default=None,
        description="Alternate allele count for male samples of uncertain ancestry in the non_cancer subset",
    )
    non_cancer_AC_popmax: int = Field(
        default=None,
        description="Allele count in the population with the maximum AF in the non_cancer subset",
    )
    non_cancer_AC_raw: int = Field(
        default=None,
        description="Alternate allele count for samples in the non_cancer subset, before removing low-confidence genotypes",
    )
    non_cancer_AC_sas: int = Field(
        default=None,
        description="Alternate allele count for samples of South Asian ancestry in the non_cancer subset",
    )
    non_cancer_AC_sas_female: int = Field(
        default=None,
        description="Alternate allele count for female samples of South Asian ancestry in the non_cancer subset",
    )
    non_cancer_AC_sas_male: int = Field(
        default=None,
        description="Alternate allele count for male samples of South Asian ancestry in the non_cancer subset",
    )
    non_cancer_AF: float = Field(
        default=None,
        description="Alternate allele frequency in samples in the non_cancer subset",
    )
    non_cancer_AF_afr: float = Field(
        default=None,
        description="Alternate allele frequency in samples of African-American ancestry in the non_cancer subset",
    )
    non_cancer_AF_afr_female: float = Field(
        default=None,
        description="Alternate allele frequency in female samples of African-American ancestry in the non_cancer subset",
    )
    non_cancer_AF_afr_male: float = Field(
        default=None,
        description="Alternate allele frequency in male samples of African-American ancestry in the non_cancer subset",
    )
    non_cancer_AF_amr: float = Field(
        default=None,
        description="Alternate allele frequency in samples of Latino ancestry in the non_cancer subset",
    )
    non_cancer_AF_amr_female: float = Field(
        default=None,
        description="Alternate allele frequency in female samples of Latino ancestry in the non_cancer subset",
    )
    non_cancer_AF_amr_male: float = Field(
        default=None,
        description="Alternate allele frequency in male samples of Latino ancestry in the non_cancer subset",
    )
    non_cancer_AF_asj: float = Field(
        default=None,
        description="Alternate allele frequency in samples of Ashkenazi Jewish ancestry in the non_cancer subset",
    )
    non_cancer_AF_asj_female: float = Field(
        default=None,
        description="Alternate allele frequency in female samples of Ashkenazi Jewish ancestry in the non_cancer subset",
    )
    non_cancer_AF_asj_male: float = Field(
        default=None,
        description="Alternate allele frequency in male samples of Ashkenazi Jewish ancestry in the non_cancer subset",
    )
    non_cancer_AF_eas: float = Field(
        default=None,
        description="Alternate allele frequency in samples of East Asian ancestry in the non_cancer subset",
    )
    non_cancer_AF_eas_female: float = Field(
        default=None,
        description="Alternate allele frequency in female samples of East Asian ancestry in the non_cancer subset",
    )
    non_cancer_AF_eas_jpn: float = Field(
        default=None,
        description="Alternate allele frequency in samples of Japanese ancestry in the non_cancer subset",
    )
    non_cancer_AF_eas_kor: float = Field(
        default=None,
        description="Alternate allele frequency in samples of Korean ancestry in the non_cancer subset",
    )
    non_cancer_AF_eas_male: float = Field(
        default=None,
        description="Alternate allele frequency in male samples of East Asian ancestry in the non_cancer subset",
    )
    non_cancer_AF_eas_oea: float = Field(
        default=None,
        description="Alternate allele frequency in samples of non-Korean, non-Japanese East Asian ancestry in the non_cancer subset",
    )
    non_cancer_AF_female: float = Field(
        default=None,
        description="Alternate allele frequency in female samples in the non_cancer subset",
    )
    non_cancer_AF_fin: float = Field(
        default=None,
        description="Alternate allele frequency in samples of Finnish ancestry in the non_cancer subset",
    )
    non_cancer_AF_fin_female: float = Field(
        default=None,
        description="Alternate allele frequency in female samples of Finnish ancestry in the non_cancer subset",
    )
    non_cancer_AF_fin_male: float = Field(
        default=None,
        description="Alternate allele frequency in male samples of Finnish ancestry in the non_cancer subset",
    )
    non_cancer_AF_male: float = Field(
        default=None,
        description="Alternate allele frequency in male samples in the non_cancer subset",
    )
    non_cancer_AF_nfe: float = Field(
        default=None,
        description="Alternate allele frequency in samples of non-Finnish European ancestry in the non_cancer subset",
    )
    non_cancer_AF_nfe_bgr: float = Field(
        default=None,
        description="Alternate allele frequency in samples of Bulgarian ancestry in the non_cancer subset",
    )
    non_cancer_AF_nfe_est: float = Field(
        default=None,
        description="Alternate allele frequency in samples of Estonian ancestry in the non_cancer subset",
    )
    non_cancer_AF_nfe_female: float = Field(
        default=None,
        description="Alternate allele frequency in female samples of non-Finnish European ancestry in the non_cancer subset",
    )
    non_cancer_AF_nfe_male: float = Field(
        default=None,
        description="Alternate allele frequency in male samples of non-Finnish European ancestry in the non_cancer subset",
    )
    non_cancer_AF_nfe_nwe: float = Field(
        default=None,
        description="Alternate allele frequency in samples of North-Western European ancestry in the non_cancer subset",
    )
    non_cancer_AF_nfe_onf: float = Field(
        default=None,
        description="Alternate allele frequency in samples of non-Finnish but otherwise indeterminate European ancestry in the non_cancer subset",
    )
    non_cancer_AF_nfe_seu: float = Field(
        default=None,
        description="Alternate allele frequency in samples of Southern European ancestry in the non_cancer subset",
    )
    non_cancer_AF_nfe_swe: float = Field(
        default=None,
        description="Alternate allele frequency in samples of Swedish ancestry in the non_cancer subset",
    )
    non_cancer_AF_oth: float = Field(
        default=None,
        description="Alternate allele frequency in samples of uncertain ancestry in the non_cancer subset",
    )
    non_cancer_AF_oth_female: float = Field(
        default=None,
        description="Alternate allele frequency in female samples of uncertain ancestry in the non_cancer subset",
    )
    non_cancer_AF_oth_male: float = Field(
        default=None,
        description="Alternate allele frequency in male samples of uncertain ancestry in the non_cancer subset",
    )
    non_cancer_AF_popmax: float = Field(
        default=None,
        description="Maximum allele frequency across populations (excluding samples of Ashkenazi, Finnish, and indeterminate ancestry) in the non_cancer subset",
    )
    non_cancer_AF_raw: float = Field(
        default=None,
        description="Alternate allele frequency in samples in the non_cancer subset, before removing low-confidence genotypes",
    )
    non_cancer_AF_sas: float = Field(
        default=None,
        description="Alternate allele frequency in samples of South Asian ancestry in the non_cancer subset",
    )
    non_cancer_AF_sas_female: float = Field(
        default=None,
        description="Alternate allele frequency in female samples of South Asian ancestry in the non_cancer subset",
    )
    non_cancer_AF_sas_male: float = Field(
        default=None,
        description="Alternate allele frequency in male samples of South Asian ancestry in the non_cancer subset",
    )
    non_cancer_AN: int = Field(
        default=None,
        description="Total number of alleles in samples in the non_cancer subset",
    )
    non_cancer_AN_afr: int = Field(
        default=None,
        description="Total number of alleles in samples of African-American ancestry in the non_cancer subset",
    )
    non_cancer_AN_afr_female: int = Field(
        default=None,
        description="Total number of alleles in female samples of African-American ancestry in the non_cancer subset",
    )
    non_cancer_AN_afr_male: int = Field(
        default=None,
        description="Total number of alleles in male samples of African-American ancestry in the non_cancer subset",
    )
    non_cancer_AN_amr: int = Field(
        default=None,
        description="Total number of alleles in samples of Latino ancestry in the non_cancer subset",
    )
    non_cancer_AN_amr_female: int = Field(
        default=None,
        description="Total number of alleles in female samples of Latino ancestry in the non_cancer subset",
    )
    non_cancer_AN_amr_male: int = Field(
        default=None,
        description="Total number of alleles in male samples of Latino ancestry in the non_cancer subset",
    )
    non_cancer_AN_asj: int = Field(
        default=None,
        description="Total number of alleles in samples of Ashkenazi Jewish ancestry in the non_cancer subset",
    )
    non_cancer_AN_asj_female: int = Field(
        default=None,
        description="Total number of alleles in female samples of Ashkenazi Jewish ancestry in the non_cancer subset",
    )
    non_cancer_AN_asj_male: int = Field(
        default=None,
        description="Total number of alleles in male samples of Ashkenazi Jewish ancestry in the non_cancer subset",
    )
    non_cancer_AN_eas: int = Field(
        default=None,
        description="Total number of alleles in samples of East Asian ancestry in the non_cancer subset",
    )
    non_cancer_AN_eas_female: int = Field(
        default=None,
        description="Total number of alleles in female samples of East Asian ancestry in the non_cancer subset",
    )
    non_cancer_AN_eas_jpn: int = Field(
        default=None,
        description="Total number of alleles in samples of Japanese ancestry in the non_cancer subset",
    )
    non_cancer_AN_eas_kor: int = Field(
        default=None,
        description="Total number of alleles in samples of Korean ancestry in the non_cancer subset",
    )
    non_cancer_AN_eas_male: int = Field(
        default=None,
        description="Total number of alleles in male samples of East Asian ancestry in the non_cancer subset",
    )
    non_cancer_AN_eas_oea: int = Field(
        default=None,
        description="Total number of alleles in samples of non-Korean, non-Japanese East Asian ancestry in the non_cancer subset",
    )
    non_cancer_AN_female: int = Field(
        default=None,
        description="Total number of alleles in female samples in the non_cancer subset",
    )
    non_cancer_AN_fin: int = Field(
        default=None,
        description="Total number of alleles in samples of Finnish ancestry in the non_cancer subset",
    )
    non_cancer_AN_fin_female: int = Field(
        default=None,
        description="Total number of alleles in female samples of Finnish ancestry in the non_cancer subset",
    )
    non_cancer_AN_fin_male: int = Field(
        default=None,
        description="Total number of alleles in male samples of Finnish ancestry in the non_cancer subset",
    )
    non_cancer_AN_male: int = Field(
        default=None,
        description="Total number of alleles in male samples in the non_cancer subset",
    )
    non_cancer_AN_nfe: int = Field(
        default=None,
        description="Total number of alleles in samples of non-Finnish European ancestry in the non_cancer subset",
    )
    non_cancer_AN_nfe_bgr: int = Field(
        default=None,
        description="Total number of alleles in samples of Bulgarian ancestry in the non_cancer subset",
    )
    non_cancer_AN_nfe_est: int = Field(
        default=None,
        description="Total number of alleles in samples of Estonian ancestry in the non_cancer subset",
    )
    non_cancer_AN_nfe_female: int = Field(
        default=None,
        description="Total number of alleles in female samples of non-Finnish European ancestry in the non_cancer subset",
    )
    non_cancer_AN_nfe_male: int = Field(
        default=None,
        description="Total number of alleles in male samples of non-Finnish European ancestry in the non_cancer subset",
    )
    non_cancer_AN_nfe_nwe: int = Field(
        default=None,
        description="Total number of alleles in samples of North-Western European ancestry in the non_cancer subset",
    )
    non_cancer_AN_nfe_onf: int = Field(
        default=None,
        description="Total number of alleles in samples of non-Finnish but otherwise indeterminate European ancestry in the non_cancer subset",
    )
    non_cancer_AN_nfe_seu: int = Field(
        default=None,
        description="Total number of alleles in samples of Southern European ancestry in the non_cancer subset",
    )
    non_cancer_AN_nfe_swe: int = Field(
        default=None,
        description="Total number of alleles in samples of Swedish ancestry in the non_cancer subset",
    )
    non_cancer_AN_oth: int = Field(
        default=None,
        description="Total number of alleles in samples of uncertain ancestry in the non_cancer subset",
    )
    non_cancer_AN_oth_female: int = Field(
        default=None,
        description="Total number of alleles in female samples of uncertain ancestry in the non_cancer subset",
    )
    non_cancer_AN_oth_male: int = Field(
        default=None,
        description="Total number of alleles in male samples of uncertain ancestry in the non_cancer subset",
    )
    non_cancer_AN_popmax: int = Field(
        default=None,
        description="Total number of alleles in the population with the maximum AF in the non_cancer subset",
    )
    non_cancer_AN_raw: int = Field(
        default=None,
        description="Total number of alleles in samples in the non_cancer subset, before removing low-confidence genotypes",
    )
    non_cancer_AN_sas: int = Field(
        default=None,
        description="Total number of alleles in samples of South Asian ancestry in the non_cancer subset",
    )
    non_cancer_AN_sas_female: int = Field(
        default=None,
        description="Total number of alleles in female samples of South Asian ancestry in the non_cancer subset",
    )
    non_cancer_AN_sas_male: int = Field(
        default=None,
        description="Total number of alleles in male samples of South Asian ancestry in the non_cancer subset",
    )
    non_cancer_faf95: float = Field(
        default=None,
        description="Filtering allele frequency (using Poisson 95% CI) for samples in the non_cancer subset",
    )
    non_cancer_faf95_afr: float = Field(
        default=None,
        description="Filtering allele frequency (using Poisson 95% CI) for samples of African-American ancestry in the non_cancer subset",
    )
    non_cancer_faf95_amr: float = Field(
        default=None,
        description="Filtering allele frequency (using Poisson 95% CI) for samples of Latino ancestry in the non_cancer subset",
    )
    non_cancer_faf95_eas: float = Field(
        default=None,
        description="Filtering allele frequency (using Poisson 95% CI) for samples of East Asian ancestry in the non_cancer subset",
    )
    non_cancer_faf95_nfe: float = Field(
        default=None,
        description="Filtering allele frequency (using Poisson 95% CI) for samples of non-Finnish European ancestry in the non_cancer subset",
    )
    non_cancer_faf95_sas: float = Field(
        default=None,
        description="Filtering allele frequency (using Poisson 95% CI) for samples of South Asian ancestry in the non_cancer subset",
    )
    non_cancer_faf99: float = Field(
        default=None,
        description="Filtering allele frequency (using Poisson 99% CI) for samples in the non_cancer subset",
    )
    non_cancer_faf99_afr: float = Field(
        default=None,
        description="Filtering allele frequency (using Poisson 99% CI) for samples of African-American ancestry in the non_cancer subset",
    )
    non_cancer_faf99_amr: float = Field(
        default=None,
        description="Filtering allele frequency (using Poisson 99% CI) for samples of Latino ancestry in the non_cancer subset",
    )
    non_cancer_faf99_eas: float = Field(
        default=None,
        description="Filtering allele frequency (using Poisson 99% CI) for samples of East Asian ancestry in the non_cancer subset",
    )
    non_cancer_faf99_nfe: float = Field(
        default=None,
        description="Filtering allele frequency (using Poisson 99% CI) for samples of non-Finnish European ancestry in the non_cancer subset",
    )
    non_cancer_faf99_sas: float = Field(
        default=None,
        description="Filtering allele frequency (using Poisson 99% CI) for samples of South Asian ancestry in the non_cancer subset",
    )
    non_cancer_nhomalt: int = Field(
        default=None,
        description="Count of homozygous individuals in samples in the non_cancer subset",
    )
    non_cancer_nhomalt_afr: int = Field(
        default=None,
        description="Count of homozygous individuals in samples of African-American ancestry in the non_cancer subset",
    )
    non_cancer_nhomalt_afr_female: int = Field(
        default=None,
        description="Count of homozygous individuals in female samples of African-American ancestry in the non_cancer subset",
    )
    non_cancer_nhomalt_afr_male: int = Field(
        default=None,
        description="Count of homozygous individuals in male samples of African-American ancestry in the non_cancer subset",
    )
    non_cancer_nhomalt_amr: int = Field(
        default=None,
        description="Count of homozygous individuals in samples of Latino ancestry in the non_cancer subset",
    )
    non_cancer_nhomalt_amr_female: int = Field(
        default=None,
        description="Count of homozygous individuals in female samples of Latino ancestry in the non_cancer subset",
    )
    non_cancer_nhomalt_amr_male: int = Field(
        default=None,
        description="Count of homozygous individuals in male samples of Latino ancestry in the non_cancer subset",
    )
    non_cancer_nhomalt_asj: int = Field(
        default=None,
        description="Count of homozygous individuals in samples of Ashkenazi Jewish ancestry in the non_cancer subset",
    )
    non_cancer_nhomalt_asj_female: int = Field(
        default=None,
        description="Count of homozygous individuals in female samples of Ashkenazi Jewish ancestry in the non_cancer subset",
    )
    non_cancer_nhomalt_asj_male: int = Field(
        default=None,
        description="Count of homozygous individuals in male samples of Ashkenazi Jewish ancestry in the non_cancer subset",
    )
    non_cancer_nhomalt_eas: int = Field(
        default=None,
        description="Count of homozygous individuals in samples of East Asian ancestry in the non_cancer subset",
    )
    non_cancer_nhomalt_eas_female: int = Field(
        default=None,
        description="Count of homozygous individuals in female samples of East Asian ancestry in the non_cancer subset",
    )
    non_cancer_nhomalt_eas_jpn: int = Field(
        default=None,
        description="Count of homozygous individuals in samples of Japanese ancestry in the non_cancer subset",
    )
    non_cancer_nhomalt_eas_kor: int = Field(
        default=None,
        description="Count of homozygous individuals in samples of Korean ancestry in the non_cancer subset",
    )
    non_cancer_nhomalt_eas_male: int = Field(
        default=None,
        description="Count of homozygous individuals in male samples of East Asian ancestry in the non_cancer subset",
    )
    non_cancer_nhomalt_eas_oea: int = Field(
        default=None,
        description="Count of homozygous individuals in samples of non-Korean, non-Japanese East Asian ancestry in the non_cancer subset",
    )
    non_cancer_nhomalt_female: int = Field(
        default=None,
        description="Count of homozygous individuals in female samples in the non_cancer subset",
    )
    non_cancer_nhomalt_fin: int = Field(
        default=None,
        description="Count of homozygous individuals in samples of Finnish ancestry in the non_cancer subset",
    )
    non_cancer_nhomalt_fin_female: int = Field(
        default=None,
        description="Count of homozygous individuals in female samples of Finnish ancestry in the non_cancer subset",
    )
    non_cancer_nhomalt_fin_male: int = Field(
        default=None,
        description="Count of homozygous individuals in male samples of Finnish ancestry in the non_cancer subset",
    )
    non_cancer_nhomalt_male: int = Field(
        default=None,
        description="Count of homozygous individuals in male samples in the non_cancer subset",
    )
    non_cancer_nhomalt_nfe: int = Field(
        default=None,
        description="Count of homozygous individuals in samples of non-Finnish European ancestry in the non_cancer subset",
    )
    non_cancer_nhomalt_nfe_bgr: int = Field(
        default=None,
        description="Count of homozygous individuals in samples of Bulgarian ancestry in the non_cancer subset",
    )
    non_cancer_nhomalt_nfe_est: int = Field(
        default=None,
        description="Count of homozygous individuals in samples of Estonian ancestry in the non_cancer subset",
    )
    non_cancer_nhomalt_nfe_female: int = Field(
        default=None,
        description="Count of homozygous individuals in female samples of non-Finnish European ancestry in the non_cancer subset",
    )
    non_cancer_nhomalt_nfe_male: int = Field(
        default=None,
        description="Count of homozygous individuals in male samples of non-Finnish European ancestry in the non_cancer subset",
    )
    non_cancer_nhomalt_nfe_nwe: int = Field(
        default=None,
        description="Count of homozygous individuals in samples of North-Western European ancestry in the non_cancer subset",
    )
    non_cancer_nhomalt_nfe_onf: int = Field(
        default=None,
        description="Count of homozygous individuals in samples of non-Finnish but otherwise indeterminate European ancestry in the non_cancer subset",
    )
    non_cancer_nhomalt_nfe_seu: int = Field(
        default=None,
        description="Count of homozygous individuals in samples of Southern European ancestry in the non_cancer subset",
    )
    non_cancer_nhomalt_nfe_swe: int = Field(
        default=None,
        description="Count of homozygous individuals in samples of Swedish ancestry in the non_cancer subset",
    )
    non_cancer_nhomalt_oth: int = Field(
        default=None,
        description="Count of homozygous individuals in samples of uncertain ancestry in the non_cancer subset",
    )
    non_cancer_nhomalt_oth_female: int = Field(
        default=None,
        description="Count of homozygous individuals in female samples of uncertain ancestry in the non_cancer subset",
    )
    non_cancer_nhomalt_oth_male: int = Field(
        default=None,
        description="Count of homozygous individuals in male samples of uncertain ancestry in the non_cancer subset",
    )
    non_cancer_nhomalt_popmax: int = Field(
        default=None,
        description="Count of homozygous individuals in the population with the maximum allele frequency in the non_cancer subset",
    )
    non_cancer_nhomalt_raw: int = Field(
        default=None,
        description="Count of homozygous individuals in samples in the non_cancer subset, before removing low-confidence genotypes",
    )
    non_cancer_nhomalt_sas: int = Field(
        default=None,
        description="Count of homozygous individuals in samples of South Asian ancestry in the non_cancer subset",
    )
    non_cancer_nhomalt_sas_female: int = Field(
        default=None,
        description="Count of homozygous individuals in female samples of South Asian ancestry in the non_cancer subset",
    )
    non_cancer_nhomalt_sas_male: int = Field(
        default=None,
        description="Count of homozygous individuals in male samples of South Asian ancestry in the non_cancer subset",
    )
    non_cancer_popmax: str = Field(
        default=None, description="Population with maximum AF in the non_cancer subset"
    )
    non_neuro_AC: int = Field(
        default=None,
        description="Alternate allele count for samples in the non_neuro subset",
    )
    non_neuro_AC_afr: int = Field(
        default=None,
        description="Alternate allele count for samples of African-American ancestry in the non_neuro subset",
    )
    non_neuro_AC_afr_female: int = Field(
        default=None,
        description="Alternate allele count for female samples of African-American ancestry in the non_neuro subset",
    )
    non_neuro_AC_afr_male: int = Field(
        default=None,
        description="Alternate allele count for male samples of African-American ancestry in the non_neuro subset",
    )
    non_neuro_AC_amr: int = Field(
        default=None,
        description="Alternate allele count for samples of Latino ancestry in the non_neuro subset",
    )
    non_neuro_AC_amr_female: int = Field(
        default=None,
        description="Alternate allele count for female samples of Latino ancestry in the non_neuro subset",
    )
    non_neuro_AC_amr_male: int = Field(
        default=None,
        description="Alternate allele count for male samples of Latino ancestry in the non_neuro subset",
    )
    non_neuro_AC_asj: int = Field(
        default=None,
        description="Alternate allele count for samples of Ashkenazi Jewish ancestry in the non_neuro subset",
    )
    non_neuro_AC_asj_female: int = Field(
        default=None,
        description="Alternate allele count for female samples of Ashkenazi Jewish ancestry in the non_neuro subset",
    )
    non_neuro_AC_asj_male: int = Field(
        default=None,
        description="Alternate allele count for male samples of Ashkenazi Jewish ancestry in the non_neuro subset",
    )
    non_neuro_AC_eas: int = Field(
        default=None,
        description="Alternate allele count for samples of East Asian ancestry in the non_neuro subset",
    )
    non_neuro_AC_eas_female: int = Field(
        default=None,
        description="Alternate allele count for female samples of East Asian ancestry in the non_neuro subset",
    )
    non_neuro_AC_eas_jpn: int = Field(
        default=None,
        description="Alternate allele count for samples of Japanese ancestry in the non_neuro subset",
    )
    non_neuro_AC_eas_kor: int = Field(
        default=None,
        description="Alternate allele count for samples of Korean ancestry in the non_neuro subset",
    )
    non_neuro_AC_eas_male: int = Field(
        default=None,
        description="Alternate allele count for male samples of East Asian ancestry in the non_neuro subset",
    )
    non_neuro_AC_eas_oea: int = Field(
        default=None,
        description="Alternate allele count for samples of non-Korean, non-Japanese East Asian ancestry in the non_neuro subset",
    )
    non_neuro_AC_female: int = Field(
        default=None,
        description="Alternate allele count for female samples in the non_neuro subset",
    )
    non_neuro_AC_fin: int = Field(
        default=None,
        description="Alternate allele count for samples of Finnish ancestry in the non_neuro subset",
    )
    non_neuro_AC_fin_female: int = Field(
        default=None,
        description="Alternate allele count for female samples of Finnish ancestry in the non_neuro subset",
    )
    non_neuro_AC_fin_male: int = Field(
        default=None,
        description="Alternate allele count for male samples of Finnish ancestry in the non_neuro subset",
    )
    non_neuro_AC_male: int = Field(
        default=None,
        description="Alternate allele count for male samples in the non_neuro subset",
    )
    non_neuro_AC_nfe: int = Field(
        default=None,
        description="Alternate allele count for samples of non-Finnish European ancestry in the non_neuro subset",
    )
    non_neuro_AC_nfe_bgr: int = Field(
        default=None,
        description="Alternate allele count for samples of Bulgarian ancestry in the non_neuro subset",
    )
    non_neuro_AC_nfe_est: int = Field(
        default=None,
        description="Alternate allele count for samples of Estonian ancestry in the non_neuro subset",
    )
    non_neuro_AC_nfe_female: int = Field(
        default=None,
        description="Alternate allele count for female samples of non-Finnish European ancestry in the non_neuro subset",
    )
    non_neuro_AC_nfe_male: int = Field(
        default=None,
        description="Alternate allele count for male samples of non-Finnish European ancestry in the non_neuro subset",
    )
    non_neuro_AC_nfe_nwe: int = Field(
        default=None,
        description="Alternate allele count for samples of North-Western European ancestry in the non_neuro subset",
    )
    non_neuro_AC_nfe_onf: int = Field(
        default=None,
        description="Alternate allele count for samples of non-Finnish but otherwise indeterminate European ancestry in the non_neuro subset",
    )
    non_neuro_AC_nfe_seu: int = Field(
        default=None,
        description="Alternate allele count for samples of Southern European ancestry in the non_neuro subset",
    )
    non_neuro_AC_nfe_swe: int = Field(
        default=None,
        description="Alternate allele count for samples of Swedish ancestry in the non_neuro subset",
    )
    non_neuro_AC_oth: int = Field(
        default=None,
        description="Alternate allele count for samples of uncertain ancestry in the non_neuro subset",
    )
    non_neuro_AC_oth_female: int = Field(
        default=None,
        description="Alternate allele count for female samples of uncertain ancestry in the non_neuro subset",
    )
    non_neuro_AC_oth_male: int = Field(
        default=None,
        description="Alternate allele count for male samples of uncertain ancestry in the non_neuro subset",
    )
    non_neuro_AC_popmax: int = Field(
        default=None,
        description="Allele count in the population with the maximum AF in the non_neuro subset",
    )
    non_neuro_AC_raw: int = Field(
        default=None,
        description="Alternate allele count for samples in the non_neuro subset, before removing low-confidence genotypes",
    )
    non_neuro_AC_sas: int = Field(
        default=None,
        description="Alternate allele count for samples of South Asian ancestry in the non_neuro subset",
    )
    non_neuro_AC_sas_female: int = Field(
        default=None,
        description="Alternate allele count for female samples of South Asian ancestry in the non_neuro subset",
    )
    non_neuro_AC_sas_male: int = Field(
        default=None,
        description="Alternate allele count for male samples of South Asian ancestry in the non_neuro subset",
    )
    non_neuro_AF: float = Field(
        default=None,
        description="Alternate allele frequency in samples in the non_neuro subset",
    )
    non_neuro_AF_afr: float = Field(
        default=None,
        description="Alternate allele frequency in samples of African-American ancestry in the non_neuro subset",
    )
    non_neuro_AF_afr_female: float = Field(
        default=None,
        description="Alternate allele frequency in female samples of African-American ancestry in the non_neuro subset",
    )
    non_neuro_AF_afr_male: float = Field(
        default=None,
        description="Alternate allele frequency in male samples of African-American ancestry in the non_neuro subset",
    )
    non_neuro_AF_amr: float = Field(
        default=None,
        description="Alternate allele frequency in samples of Latino ancestry in the non_neuro subset",
    )
    non_neuro_AF_amr_female: float = Field(
        default=None,
        description="Alternate allele frequency in female samples of Latino ancestry in the non_neuro subset",
    )
    non_neuro_AF_amr_male: float = Field(
        default=None,
        description="Alternate allele frequency in male samples of Latino ancestry in the non_neuro subset",
    )
    non_neuro_AF_asj: float = Field(
        default=None,
        description="Alternate allele frequency in samples of Ashkenazi Jewish ancestry in the non_neuro subset",
    )
    non_neuro_AF_asj_female: float = Field(
        default=None,
        description="Alternate allele frequency in female samples of Ashkenazi Jewish ancestry in the non_neuro subset",
    )
    non_neuro_AF_asj_male: float = Field(
        default=None,
        description="Alternate allele frequency in male samples of Ashkenazi Jewish ancestry in the non_neuro subset",
    )
    non_neuro_AF_eas: float = Field(
        default=None,
        description="Alternate allele frequency in samples of East Asian ancestry in the non_neuro subset",
    )
    non_neuro_AF_eas_female: float = Field(
        default=None,
        description="Alternate allele frequency in female samples of East Asian ancestry in the non_neuro subset",
    )
    non_neuro_AF_eas_jpn: float = Field(
        default=None,
        description="Alternate allele frequency in samples of Japanese ancestry in the non_neuro subset",
    )
    non_neuro_AF_eas_kor: float = Field(
        default=None,
        description="Alternate allele frequency in samples of Korean ancestry in the non_neuro subset",
    )
    non_neuro_AF_eas_male: float = Field(
        default=None,
        description="Alternate allele frequency in male samples of East Asian ancestry in the non_neuro subset",
    )
    non_neuro_AF_eas_oea: float = Field(
        default=None,
        description="Alternate allele frequency in samples of non-Korean, non-Japanese East Asian ancestry in the non_neuro subset",
    )
    non_neuro_AF_female: float = Field(
        default=None,
        description="Alternate allele frequency in female samples in the non_neuro subset",
    )
    non_neuro_AF_fin: float = Field(
        default=None,
        description="Alternate allele frequency in samples of Finnish ancestry in the non_neuro subset",
    )
    non_neuro_AF_fin_female: float = Field(
        default=None,
        description="Alternate allele frequency in female samples of Finnish ancestry in the non_neuro subset",
    )
    non_neuro_AF_fin_male: float = Field(
        default=None,
        description="Alternate allele frequency in male samples of Finnish ancestry in the non_neuro subset",
    )
    non_neuro_AF_male: float = Field(
        default=None,
        description="Alternate allele frequency in male samples in the non_neuro subset",
    )
    non_neuro_AF_nfe: float = Field(
        default=None,
        description="Alternate allele frequency in samples of non-Finnish European ancestry in the non_neuro subset",
    )
    non_neuro_AF_nfe_bgr: float = Field(
        default=None,
        description="Alternate allele frequency in samples of Bulgarian ancestry in the non_neuro subset",
    )
    non_neuro_AF_nfe_est: float = Field(
        default=None,
        description="Alternate allele frequency in samples of Estonian ancestry in the non_neuro subset",
    )
    non_neuro_AF_nfe_female: float = Field(
        default=None,
        description="Alternate allele frequency in female samples of non-Finnish European ancestry in the non_neuro subset",
    )
    non_neuro_AF_nfe_male: float = Field(
        default=None,
        description="Alternate allele frequency in male samples of non-Finnish European ancestry in the non_neuro subset",
    )
    non_neuro_AF_nfe_nwe: float = Field(
        default=None,
        description="Alternate allele frequency in samples of North-Western European ancestry in the non_neuro subset",
    )
    non_neuro_AF_nfe_onf: float = Field(
        default=None,
        description="Alternate allele frequency in samples of non-Finnish but otherwise indeterminate European ancestry in the non_neuro subset",
    )
    non_neuro_AF_nfe_seu: float = Field(
        default=None,
        description="Alternate allele frequency in samples of Southern European ancestry in the non_neuro subset",
    )
    non_neuro_AF_nfe_swe: float = Field(
        default=None,
        description="Alternate allele frequency in samples of Swedish ancestry in the non_neuro subset",
    )
    non_neuro_AF_oth: float = Field(
        default=None,
        description="Alternate allele frequency in samples of uncertain ancestry in the non_neuro subset",
    )
    non_neuro_AF_oth_female: float = Field(
        default=None,
        description="Alternate allele frequency in female samples of uncertain ancestry in the non_neuro subset",
    )
    non_neuro_AF_oth_male: float = Field(
        default=None,
        description="Alternate allele frequency in male samples of uncertain ancestry in the non_neuro subset",
    )
    non_neuro_AF_popmax: float = Field(
        default=None,
        description="Maximum allele frequency across populations (excluding samples of Ashkenazi, Finnish, and indeterminate ancestry) in the non_neuro subset",
    )
    non_neuro_AF_raw: float = Field(
        default=None,
        description="Alternate allele frequency in samples in the non_neuro subset, before removing low-confidence genotypes",
    )
    non_neuro_AF_sas: float = Field(
        default=None,
        description="Alternate allele frequency in samples of South Asian ancestry in the non_neuro subset",
    )
    non_neuro_AF_sas_female: float = Field(
        default=None,
        description="Alternate allele frequency in female samples of South Asian ancestry in the non_neuro subset",
    )
    non_neuro_AF_sas_male: float = Field(
        default=None,
        description="Alternate allele frequency in male samples of South Asian ancestry in the non_neuro subset",
    )
    non_neuro_AN: int = Field(
        default=None,
        description="Total number of alleles in samples in the non_neuro subset",
    )
    non_neuro_AN_afr: int = Field(
        default=None,
        description="Total number of alleles in samples of African-American ancestry in the non_neuro subset",
    )
    non_neuro_AN_afr_female: int = Field(
        default=None,
        description="Total number of alleles in female samples of African-American ancestry in the non_neuro subset",
    )
    non_neuro_AN_afr_male: int = Field(
        default=None,
        description="Total number of alleles in male samples of African-American ancestry in the non_neuro subset",
    )
    non_neuro_AN_amr: int = Field(
        default=None,
        description="Total number of alleles in samples of Latino ancestry in the non_neuro subset",
    )
    non_neuro_AN_amr_female: int = Field(
        default=None,
        description="Total number of alleles in female samples of Latino ancestry in the non_neuro subset",
    )
    non_neuro_AN_amr_male: int = Field(
        default=None,
        description="Total number of alleles in male samples of Latino ancestry in the non_neuro subset",
    )
    non_neuro_AN_asj: int = Field(
        default=None,
        description="Total number of alleles in samples of Ashkenazi Jewish ancestry in the non_neuro subset",
    )
    non_neuro_AN_asj_female: int = Field(
        default=None,
        description="Total number of alleles in female samples of Ashkenazi Jewish ancestry in the non_neuro subset",
    )
    non_neuro_AN_asj_male: int = Field(
        default=None,
        description="Total number of alleles in male samples of Ashkenazi Jewish ancestry in the non_neuro subset",
    )
    non_neuro_AN_eas: int = Field(
        default=None,
        description="Total number of alleles in samples of East Asian ancestry in the non_neuro subset",
    )
    non_neuro_AN_eas_female: int = Field(
        default=None,
        description="Total number of alleles in female samples of East Asian ancestry in the non_neuro subset",
    )
    non_neuro_AN_eas_jpn: int = Field(
        default=None,
        description="Total number of alleles in samples of Japanese ancestry in the non_neuro subset",
    )
    non_neuro_AN_eas_kor: int = Field(
        default=None,
        description="Total number of alleles in samples of Korean ancestry in the non_neuro subset",
    )
    non_neuro_AN_eas_male: int = Field(
        default=None,
        description="Total number of alleles in male samples of East Asian ancestry in the non_neuro subset",
    )
    non_neuro_AN_eas_oea: int = Field(
        default=None,
        description="Total number of alleles in samples of non-Korean, non-Japanese East Asian ancestry in the non_neuro subset",
    )
    non_neuro_AN_female: int = Field(
        default=None,
        description="Total number of alleles in female samples in the non_neuro subset",
    )
    non_neuro_AN_fin: int = Field(
        default=None,
        description="Total number of alleles in samples of Finnish ancestry in the non_neuro subset",
    )
    non_neuro_AN_fin_female: int = Field(
        default=None,
        description="Total number of alleles in female samples of Finnish ancestry in the non_neuro subset",
    )
    non_neuro_AN_fin_male: int = Field(
        default=None,
        description="Total number of alleles in male samples of Finnish ancestry in the non_neuro subset",
    )
    non_neuro_AN_male: int = Field(
        default=None,
        description="Total number of alleles in male samples in the non_neuro subset",
    )
    non_neuro_AN_nfe: int = Field(
        default=None,
        description="Total number of alleles in samples of non-Finnish European ancestry in the non_neuro subset",
    )
    non_neuro_AN_nfe_bgr: int = Field(
        default=None,
        description="Total number of alleles in samples of Bulgarian ancestry in the non_neuro subset",
    )
    non_neuro_AN_nfe_est: int = Field(
        default=None,
        description="Total number of alleles in samples of Estonian ancestry in the non_neuro subset",
    )
    non_neuro_AN_nfe_female: int = Field(
        default=None,
        description="Total number of alleles in female samples of non-Finnish European ancestry in the non_neuro subset",
    )
    non_neuro_AN_nfe_male: int = Field(
        default=None,
        description="Total number of alleles in male samples of non-Finnish European ancestry in the non_neuro subset",
    )
    non_neuro_AN_nfe_nwe: int = Field(
        default=None,
        description="Total number of alleles in samples of North-Western European ancestry in the non_neuro subset",
    )
    non_neuro_AN_nfe_onf: int = Field(
        default=None,
        description="Total number of alleles in samples of non-Finnish but otherwise indeterminate European ancestry in the non_neuro subset",
    )
    non_neuro_AN_nfe_seu: int = Field(
        default=None,
        description="Total number of alleles in samples of Southern European ancestry in the non_neuro subset",
    )
    non_neuro_AN_nfe_swe: int = Field(
        default=None,
        description="Total number of alleles in samples of Swedish ancestry in the non_neuro subset",
    )
    non_neuro_AN_oth: int = Field(
        default=None,
        description="Total number of alleles in samples of uncertain ancestry in the non_neuro subset",
    )
    non_neuro_AN_oth_female: int = Field(
        default=None,
        description="Total number of alleles in female samples of uncertain ancestry in the non_neuro subset",
    )
    non_neuro_AN_oth_male: int = Field(
        default=None,
        description="Total number of alleles in male samples of uncertain ancestry in the non_neuro subset",
    )
    non_neuro_AN_popmax: int = Field(
        default=None,
        description="Total number of alleles in the population with the maximum AF in the non_neuro subset",
    )
    non_neuro_AN_raw: int = Field(
        default=None,
        description="Total number of alleles in samples in the non_neuro subset, before removing low-confidence genotypes",
    )
    non_neuro_AN_sas: int = Field(
        default=None,
        description="Total number of alleles in samples of South Asian ancestry in the non_neuro subset",
    )
    non_neuro_AN_sas_female: int = Field(
        default=None,
        description="Total number of alleles in female samples of South Asian ancestry in the non_neuro subset",
    )
    non_neuro_AN_sas_male: int = Field(
        default=None,
        description="Total number of alleles in male samples of South Asian ancestry in the non_neuro subset",
    )
    non_neuro_faf95: float = Field(
        default=None,
        description="Filtering allele frequency (using Poisson 95% CI) for samples in the non_neuro subset",
    )
    non_neuro_faf95_afr: float = Field(
        default=None,
        description="Filtering allele frequency (using Poisson 95% CI) for samples of African-American ancestry in the non_neuro subset",
    )
    non_neuro_faf95_amr: float = Field(
        default=None,
        description="Filtering allele frequency (using Poisson 95% CI) for samples of Latino ancestry in the non_neuro subset",
    )
    non_neuro_faf95_eas: float = Field(
        default=None,
        description="Filtering allele frequency (using Poisson 95% CI) for samples of East Asian ancestry in the non_neuro subset",
    )
    non_neuro_faf95_nfe: float = Field(
        default=None,
        description="Filtering allele frequency (using Poisson 95% CI) for samples of non-Finnish European ancestry in the non_neuro subset",
    )
    non_neuro_faf95_sas: float = Field(
        default=None,
        description="Filtering allele frequency (using Poisson 95% CI) for samples of South Asian ancestry in the non_neuro subset",
    )
    non_neuro_faf99: float = Field(
        default=None,
        description="Filtering allele frequency (using Poisson 99% CI) for samples in the non_neuro subset",
    )
    non_neuro_faf99_afr: float = Field(
        default=None,
        description="Filtering allele frequency (using Poisson 99% CI) for samples of African-American ancestry in the non_neuro subset",
    )
    non_neuro_faf99_amr: float = Field(
        default=None,
        description="Filtering allele frequency (using Poisson 99% CI) for samples of Latino ancestry in the non_neuro subset",
    )
    non_neuro_faf99_eas: float = Field(
        default=None,
        description="Filtering allele frequency (using Poisson 99% CI) for samples of East Asian ancestry in the non_neuro subset",
    )
    non_neuro_faf99_nfe: float = Field(
        default=None,
        description="Filtering allele frequency (using Poisson 99% CI) for samples of non-Finnish European ancestry in the non_neuro subset",
    )
    non_neuro_faf99_sas: float = Field(
        default=None,
        description="Filtering allele frequency (using Poisson 99% CI) for samples of South Asian ancestry in the non_neuro subset",
    )
    non_neuro_nhomalt: int = Field(
        default=None,
        description="Count of homozygous individuals in samples in the non_neuro subset",
    )
    non_neuro_nhomalt_afr: int = Field(
        default=None,
        description="Count of homozygous individuals in samples of African-American ancestry in the non_neuro subset",
    )
    non_neuro_nhomalt_afr_female: int = Field(
        default=None,
        description="Count of homozygous individuals in female samples of African-American ancestry in the non_neuro subset",
    )
    non_neuro_nhomalt_afr_male: int = Field(
        default=None,
        description="Count of homozygous individuals in male samples of African-American ancestry in the non_neuro subset",
    )
    non_neuro_nhomalt_amr: int = Field(
        default=None,
        description="Count of homozygous individuals in samples of Latino ancestry in the non_neuro subset",
    )
    non_neuro_nhomalt_amr_female: int = Field(
        default=None,
        description="Count of homozygous individuals in female samples of Latino ancestry in the non_neuro subset",
    )
    non_neuro_nhomalt_amr_male: int = Field(
        default=None,
        description="Count of homozygous individuals in male samples of Latino ancestry in the non_neuro subset",
    )
    non_neuro_nhomalt_asj: int = Field(
        default=None,
        description="Count of homozygous individuals in samples of Ashkenazi Jewish ancestry in the non_neuro subset",
    )
    non_neuro_nhomalt_asj_female: int = Field(
        default=None,
        description="Count of homozygous individuals in female samples of Ashkenazi Jewish ancestry in the non_neuro subset",
    )
    non_neuro_nhomalt_asj_male: int = Field(
        default=None,
        description="Count of homozygous individuals in male samples of Ashkenazi Jewish ancestry in the non_neuro subset",
    )
    non_neuro_nhomalt_eas: int = Field(
        default=None,
        description="Count of homozygous individuals in samples of East Asian ancestry in the non_neuro subset",
    )
    non_neuro_nhomalt_eas_female: int = Field(
        default=None,
        description="Count of homozygous individuals in female samples of East Asian ancestry in the non_neuro subset",
    )
    non_neuro_nhomalt_eas_jpn: int = Field(
        default=None,
        description="Count of homozygous individuals in samples of Japanese ancestry in the non_neuro subset",
    )
    non_neuro_nhomalt_eas_kor: int = Field(
        default=None,
        description="Count of homozygous individuals in samples of Korean ancestry in the non_neuro subset",
    )
    non_neuro_nhomalt_eas_male: int = Field(
        default=None,
        description="Count of homozygous individuals in male samples of East Asian ancestry in the non_neuro subset",
    )
    non_neuro_nhomalt_eas_oea: int = Field(
        default=None,
        description="Count of homozygous individuals in samples of non-Korean, non-Japanese East Asian ancestry in the non_neuro subset",
    )
    non_neuro_nhomalt_female: int = Field(
        default=None,
        description="Count of homozygous individuals in female samples in the non_neuro subset",
    )
    non_neuro_nhomalt_fin: int = Field(
        default=None,
        description="Count of homozygous individuals in samples of Finnish ancestry in the non_neuro subset",
    )
    non_neuro_nhomalt_fin_female: int = Field(
        default=None,
        description="Count of homozygous individuals in female samples of Finnish ancestry in the non_neuro subset",
    )
    non_neuro_nhomalt_fin_male: int = Field(
        default=None,
        description="Count of homozygous individuals in male samples of Finnish ancestry in the non_neuro subset",
    )
    non_neuro_nhomalt_male: int = Field(
        default=None,
        description="Count of homozygous individuals in male samples in the non_neuro subset",
    )
    non_neuro_nhomalt_nfe: int = Field(
        default=None,
        description="Count of homozygous individuals in samples of non-Finnish European ancestry in the non_neuro subset",
    )
    non_neuro_nhomalt_nfe_bgr: int = Field(
        default=None,
        description="Count of homozygous individuals in samples of Bulgarian ancestry in the non_neuro subset",
    )
    non_neuro_nhomalt_nfe_est: int = Field(
        default=None,
        description="Count of homozygous individuals in samples of Estonian ancestry in the non_neuro subset",
    )
    non_neuro_nhomalt_nfe_female: int = Field(
        default=None,
        description="Count of homozygous individuals in female samples of non-Finnish European ancestry in the non_neuro subset",
    )
    non_neuro_nhomalt_nfe_male: int = Field(
        default=None,
        description="Count of homozygous individuals in male samples of non-Finnish European ancestry in the non_neuro subset",
    )
    non_neuro_nhomalt_nfe_nwe: int = Field(
        default=None,
        description="Count of homozygous individuals in samples of North-Western European ancestry in the non_neuro subset",
    )
    non_neuro_nhomalt_nfe_onf: int = Field(
        default=None,
        description="Count of homozygous individuals in samples of non-Finnish but otherwise indeterminate European ancestry in the non_neuro subset",
    )
    non_neuro_nhomalt_nfe_seu: int = Field(
        default=None,
        description="Count of homozygous individuals in samples of Southern European ancestry in the non_neuro subset",
    )
    non_neuro_nhomalt_nfe_swe: int = Field(
        default=None,
        description="Count of homozygous individuals in samples of Swedish ancestry in the non_neuro subset",
    )
    non_neuro_nhomalt_oth: int = Field(
        default=None,
        description="Count of homozygous individuals in samples of uncertain ancestry in the non_neuro subset",
    )
    non_neuro_nhomalt_oth_female: int = Field(
        default=None,
        description="Count of homozygous individuals in female samples of uncertain ancestry in the non_neuro subset",
    )
    non_neuro_nhomalt_oth_male: int = Field(
        default=None,
        description="Count of homozygous individuals in male samples of uncertain ancestry in the non_neuro subset",
    )
    non_neuro_nhomalt_popmax: int = Field(
        default=None,
        description="Count of homozygous individuals in the population with the maximum allele frequency in the non_neuro subset",
    )
    non_neuro_nhomalt_raw: int = Field(
        default=None,
        description="Count of homozygous individuals in samples in the non_neuro subset, before removing low-confidence genotypes",
    )
    non_neuro_nhomalt_sas: int = Field(
        default=None,
        description="Count of homozygous individuals in samples of South Asian ancestry in the non_neuro subset",
    )
    non_neuro_nhomalt_sas_female: int = Field(
        default=None,
        description="Count of homozygous individuals in female samples of South Asian ancestry in the non_neuro subset",
    )
    non_neuro_nhomalt_sas_male: int = Field(
        default=None,
        description="Count of homozygous individuals in male samples of South Asian ancestry in the non_neuro subset",
    )
    non_neuro_popmax: str = Field(
        default=None, description="Population with maximum AF in the non_neuro subset"
    )
    non_topmed_AC: int = Field(
        default=None,
        description="Alternate allele count for samples in the non_topmed subset",
    )
    non_topmed_AC_afr: int = Field(
        default=None,
        description="Alternate allele count for samples of African-American ancestry in the non_topmed subset",
    )
    non_topmed_AC_afr_female: int = Field(
        default=None,
        description="Alternate allele count for female samples of African-American ancestry in the non_topmed subset",
    )
    non_topmed_AC_afr_male: int = Field(
        default=None,
        description="Alternate allele count for male samples of African-American ancestry in the non_topmed subset",
    )
    non_topmed_AC_amr: int = Field(
        default=None,
        description="Alternate allele count for samples of Latino ancestry in the non_topmed subset",
    )
    non_topmed_AC_amr_female: int = Field(
        default=None,
        description="Alternate allele count for female samples of Latino ancestry in the non_topmed subset",
    )
    non_topmed_AC_amr_male: int = Field(
        default=None,
        description="Alternate allele count for male samples of Latino ancestry in the non_topmed subset",
    )
    non_topmed_AC_asj: int = Field(
        default=None,
        description="Alternate allele count for samples of Ashkenazi Jewish ancestry in the non_topmed subset",
    )
    non_topmed_AC_asj_female: int = Field(
        default=None,
        description="Alternate allele count for female samples of Ashkenazi Jewish ancestry in the non_topmed subset",
    )
    non_topmed_AC_asj_male: int = Field(
        default=None,
        description="Alternate allele count for male samples of Ashkenazi Jewish ancestry in the non_topmed subset",
    )
    non_topmed_AC_eas: int = Field(
        default=None,
        description="Alternate allele count for samples of East Asian ancestry in the non_topmed subset",
    )
    non_topmed_AC_eas_female: int = Field(
        default=None,
        description="Alternate allele count for female samples of East Asian ancestry in the non_topmed subset",
    )
    non_topmed_AC_eas_jpn: int = Field(
        default=None,
        description="Alternate allele count for samples of Japanese ancestry in the non_topmed subset",
    )
    non_topmed_AC_eas_kor: int = Field(
        default=None,
        description="Alternate allele count for samples of Korean ancestry in the non_topmed subset",
    )
    non_topmed_AC_eas_male: int = Field(
        default=None,
        description="Alternate allele count for male samples of East Asian ancestry in the non_topmed subset",
    )
    non_topmed_AC_eas_oea: int = Field(
        default=None,
        description="Alternate allele count for samples of non-Korean, non-Japanese East Asian ancestry in the non_topmed subset",
    )
    non_topmed_AC_female: int = Field(
        default=None,
        description="Alternate allele count for female samples in the non_topmed subset",
    )
    non_topmed_AC_fin: int = Field(
        default=None,
        description="Alternate allele count for samples of Finnish ancestry in the non_topmed subset",
    )
    non_topmed_AC_fin_female: int = Field(
        default=None,
        description="Alternate allele count for female samples of Finnish ancestry in the non_topmed subset",
    )
    non_topmed_AC_fin_male: int = Field(
        default=None,
        description="Alternate allele count for male samples of Finnish ancestry in the non_topmed subset",
    )
    non_topmed_AC_male: int = Field(
        default=None,
        description="Alternate allele count for male samples in the non_topmed subset",
    )
    non_topmed_AC_nfe: int = Field(
        default=None,
        description="Alternate allele count for samples of non-Finnish European ancestry in the non_topmed subset",
    )
    non_topmed_AC_nfe_bgr: int = Field(
        default=None,
        description="Alternate allele count for samples of Bulgarian ancestry in the non_topmed subset",
    )
    non_topmed_AC_nfe_est: int = Field(
        default=None,
        description="Alternate allele count for samples of Estonian ancestry in the non_topmed subset",
    )
    non_topmed_AC_nfe_female: int = Field(
        default=None,
        description="Alternate allele count for female samples of non-Finnish European ancestry in the non_topmed subset",
    )
    non_topmed_AC_nfe_male: int = Field(
        default=None,
        description="Alternate allele count for male samples of non-Finnish European ancestry in the non_topmed subset",
    )
    non_topmed_AC_nfe_nwe: int = Field(
        default=None,
        description="Alternate allele count for samples of North-Western European ancestry in the non_topmed subset",
    )
    non_topmed_AC_nfe_onf: int = Field(
        default=None,
        description="Alternate allele count for samples of non-Finnish but otherwise indeterminate European ancestry in the non_topmed subset",
    )
    non_topmed_AC_nfe_seu: int = Field(
        default=None,
        description="Alternate allele count for samples of Southern European ancestry in the non_topmed subset",
    )
    non_topmed_AC_nfe_swe: int = Field(
        default=None,
        description="Alternate allele count for samples of Swedish ancestry in the non_topmed subset",
    )
    non_topmed_AC_oth: int = Field(
        default=None,
        description="Alternate allele count for samples of uncertain ancestry in the non_topmed subset",
    )
    non_topmed_AC_oth_female: int = Field(
        default=None,
        description="Alternate allele count for female samples of uncertain ancestry in the non_topmed subset",
    )
    non_topmed_AC_oth_male: int = Field(
        default=None,
        description="Alternate allele count for male samples of uncertain ancestry in the non_topmed subset",
    )
    non_topmed_AC_popmax: int = Field(
        default=None,
        description="Allele count in the population with the maximum AF in the non_topmed subset",
    )
    non_topmed_AC_raw: int = Field(
        default=None,
        description="Alternate allele count for samples in the non_topmed subset, before removing low-confidence genotypes",
    )
    non_topmed_AC_sas: int = Field(
        default=None,
        description="Alternate allele count for samples of South Asian ancestry in the non_topmed subset",
    )
    non_topmed_AC_sas_female: int = Field(
        default=None,
        description="Alternate allele count for female samples of South Asian ancestry in the non_topmed subset",
    )
    non_topmed_AC_sas_male: int = Field(
        default=None,
        description="Alternate allele count for male samples of South Asian ancestry in the non_topmed subset",
    )
    non_topmed_AF: float = Field(
        default=None,
        description="Alternate allele frequency in samples in the non_topmed subset",
    )
    non_topmed_AF_afr: float = Field(
        default=None,
        description="Alternate allele frequency in samples of African-American ancestry in the non_topmed subset",
    )
    non_topmed_AF_afr_female: float = Field(
        default=None,
        description="Alternate allele frequency in female samples of African-American ancestry in the non_topmed subset",
    )
    non_topmed_AF_afr_male: float = Field(
        default=None,
        description="Alternate allele frequency in male samples of African-American ancestry in the non_topmed subset",
    )
    non_topmed_AF_amr: float = Field(
        default=None,
        description="Alternate allele frequency in samples of Latino ancestry in the non_topmed subset",
    )
    non_topmed_AF_amr_female: float = Field(
        default=None,
        description="Alternate allele frequency in female samples of Latino ancestry in the non_topmed subset",
    )
    non_topmed_AF_amr_male: float = Field(
        default=None,
        description="Alternate allele frequency in male samples of Latino ancestry in the non_topmed subset",
    )
    non_topmed_AF_asj: float = Field(
        default=None,
        description="Alternate allele frequency in samples of Ashkenazi Jewish ancestry in the non_topmed subset",
    )
    non_topmed_AF_asj_female: float = Field(
        default=None,
        description="Alternate allele frequency in female samples of Ashkenazi Jewish ancestry in the non_topmed subset",
    )
    non_topmed_AF_asj_male: float = Field(
        default=None,
        description="Alternate allele frequency in male samples of Ashkenazi Jewish ancestry in the non_topmed subset",
    )
    non_topmed_AF_eas: float = Field(
        default=None,
        description="Alternate allele frequency in samples of East Asian ancestry in the non_topmed subset",
    )
    non_topmed_AF_eas_female: float = Field(
        default=None,
        description="Alternate allele frequency in female samples of East Asian ancestry in the non_topmed subset",
    )
    non_topmed_AF_eas_jpn: float = Field(
        default=None,
        description="Alternate allele frequency in samples of Japanese ancestry in the non_topmed subset",
    )
    non_topmed_AF_eas_kor: float = Field(
        default=None,
        description="Alternate allele frequency in samples of Korean ancestry in the non_topmed subset",
    )
    non_topmed_AF_eas_male: float = Field(
        default=None,
        description="Alternate allele frequency in male samples of East Asian ancestry in the non_topmed subset",
    )
    non_topmed_AF_eas_oea: float = Field(
        default=None,
        description="Alternate allele frequency in samples of non-Korean, non-Japanese East Asian ancestry in the non_topmed subset",
    )
    non_topmed_AF_female: float = Field(
        default=None,
        description="Alternate allele frequency in female samples in the non_topmed subset",
    )
    non_topmed_AF_fin: float = Field(
        default=None,
        description="Alternate allele frequency in samples of Finnish ancestry in the non_topmed subset",
    )
    non_topmed_AF_fin_female: float = Field(
        default=None,
        description="Alternate allele frequency in female samples of Finnish ancestry in the non_topmed subset",
    )
    non_topmed_AF_fin_male: float = Field(
        default=None,
        description="Alternate allele frequency in male samples of Finnish ancestry in the non_topmed subset",
    )
    non_topmed_AF_male: float = Field(
        default=None,
        description="Alternate allele frequency in male samples in the non_topmed subset",
    )
    non_topmed_AF_nfe: float = Field(
        default=None,
        description="Alternate allele frequency in samples of non-Finnish European ancestry in the non_topmed subset",
    )
    non_topmed_AF_nfe_bgr: float = Field(
        default=None,
        description="Alternate allele frequency in samples of Bulgarian ancestry in the non_topmed subset",
    )
    non_topmed_AF_nfe_est: float = Field(
        default=None,
        description="Alternate allele frequency in samples of Estonian ancestry in the non_topmed subset",
    )
    non_topmed_AF_nfe_female: float = Field(
        default=None,
        description="Alternate allele frequency in female samples of non-Finnish European ancestry in the non_topmed subset",
    )
    non_topmed_AF_nfe_male: float = Field(
        default=None,
        description="Alternate allele frequency in male samples of non-Finnish European ancestry in the non_topmed subset",
    )
    non_topmed_AF_nfe_nwe: float = Field(
        default=None,
        description="Alternate allele frequency in samples of North-Western European ancestry in the non_topmed subset",
    )
    non_topmed_AF_nfe_onf: float = Field(
        default=None,
        description="Alternate allele frequency in samples of non-Finnish but otherwise indeterminate European ancestry in the non_topmed subset",
    )
    non_topmed_AF_nfe_seu: float = Field(
        default=None,
        description="Alternate allele frequency in samples of Southern European ancestry in the non_topmed subset",
    )
    non_topmed_AF_nfe_swe: float = Field(
        default=None,
        description="Alternate allele frequency in samples of Swedish ancestry in the non_topmed subset",
    )
    non_topmed_AF_oth: float = Field(
        default=None,
        description="Alternate allele frequency in samples of uncertain ancestry in the non_topmed subset",
    )
    non_topmed_AF_oth_female: float = Field(
        default=None,
        description="Alternate allele frequency in female samples of uncertain ancestry in the non_topmed subset",
    )
    non_topmed_AF_oth_male: float = Field(
        default=None,
        description="Alternate allele frequency in male samples of uncertain ancestry in the non_topmed subset",
    )
    non_topmed_AF_popmax: float = Field(
        default=None,
        description="Maximum allele frequency across populations (excluding samples of Ashkenazi, Finnish, and indeterminate ancestry) in the non_topmed subset",
    )
    non_topmed_AF_raw: float = Field(
        default=None,
        description="Alternate allele frequency in samples in the non_topmed subset, before removing low-confidence genotypes",
    )
    non_topmed_AF_sas: float = Field(
        default=None,
        description="Alternate allele frequency in samples of South Asian ancestry in the non_topmed subset",
    )
    non_topmed_AF_sas_female: float = Field(
        default=None,
        description="Alternate allele frequency in female samples of South Asian ancestry in the non_topmed subset",
    )
    non_topmed_AF_sas_male: float = Field(
        default=None,
        description="Alternate allele frequency in male samples of South Asian ancestry in the non_topmed subset",
    )
    non_topmed_AN: int = Field(
        default=None,
        description="Total number of alleles in samples in the non_topmed subset",
    )
    non_topmed_AN_afr: int = Field(
        default=None,
        description="Total number of alleles in samples of African-American ancestry in the non_topmed subset",
    )
    non_topmed_AN_afr_female: int = Field(
        default=None,
        description="Total number of alleles in female samples of African-American ancestry in the non_topmed subset",
    )
    non_topmed_AN_afr_male: int = Field(
        default=None,
        description="Total number of alleles in male samples of African-American ancestry in the non_topmed subset",
    )
    non_topmed_AN_amr: int = Field(
        default=None,
        description="Total number of alleles in samples of Latino ancestry in the non_topmed subset",
    )
    non_topmed_AN_amr_female: int = Field(
        default=None,
        description="Total number of alleles in female samples of Latino ancestry in the non_topmed subset",
    )
    non_topmed_AN_amr_male: int = Field(
        default=None,
        description="Total number of alleles in male samples of Latino ancestry in the non_topmed subset",
    )
    non_topmed_AN_asj: int = Field(
        default=None,
        description="Total number of alleles in samples of Ashkenazi Jewish ancestry in the non_topmed subset",
    )
    non_topmed_AN_asj_female: int = Field(
        default=None,
        description="Total number of alleles in female samples of Ashkenazi Jewish ancestry in the non_topmed subset",
    )
    non_topmed_AN_asj_male: int = Field(
        default=None,
        description="Total number of alleles in male samples of Ashkenazi Jewish ancestry in the non_topmed subset",
    )
    non_topmed_AN_eas: int = Field(
        default=None,
        description="Total number of alleles in samples of East Asian ancestry in the non_topmed subset",
    )
    non_topmed_AN_eas_female: int = Field(
        default=None,
        description="Total number of alleles in female samples of East Asian ancestry in the non_topmed subset",
    )
    non_topmed_AN_eas_jpn: int = Field(
        default=None,
        description="Total number of alleles in samples of Japanese ancestry in the non_topmed subset",
    )
    non_topmed_AN_eas_kor: int = Field(
        default=None,
        description="Total number of alleles in samples of Korean ancestry in the non_topmed subset",
    )
    non_topmed_AN_eas_male: int = Field(
        default=None,
        description="Total number of alleles in male samples of East Asian ancestry in the non_topmed subset",
    )
    non_topmed_AN_eas_oea: int = Field(
        default=None,
        description="Total number of alleles in samples of non-Korean, non-Japanese East Asian ancestry in the non_topmed subset",
    )
    non_topmed_AN_female: int = Field(
        default=None,
        description="Total number of alleles in female samples in the non_topmed subset",
    )
    non_topmed_AN_fin: int = Field(
        default=None,
        description="Total number of alleles in samples of Finnish ancestry in the non_topmed subset",
    )
    non_topmed_AN_fin_female: int = Field(
        default=None,
        description="Total number of alleles in female samples of Finnish ancestry in the non_topmed subset",
    )
    non_topmed_AN_fin_male: int = Field(
        default=None,
        description="Total number of alleles in male samples of Finnish ancestry in the non_topmed subset",
    )
    non_topmed_AN_male: int = Field(
        default=None,
        description="Total number of alleles in male samples in the non_topmed subset",
    )
    non_topmed_AN_nfe: int = Field(
        default=None,
        description="Total number of alleles in samples of non-Finnish European ancestry in the non_topmed subset",
    )
    non_topmed_AN_nfe_bgr: int = Field(
        default=None,
        description="Total number of alleles in samples of Bulgarian ancestry in the non_topmed subset",
    )
    non_topmed_AN_nfe_est: int = Field(
        default=None,
        description="Total number of alleles in samples of Estonian ancestry in the non_topmed subset",
    )
    non_topmed_AN_nfe_female: int = Field(
        default=None,
        description="Total number of alleles in female samples of non-Finnish European ancestry in the non_topmed subset",
    )
    non_topmed_AN_nfe_male: int = Field(
        default=None,
        description="Total number of alleles in male samples of non-Finnish European ancestry in the non_topmed subset",
    )
    non_topmed_AN_nfe_nwe: int = Field(
        default=None,
        description="Total number of alleles in samples of North-Western European ancestry in the non_topmed subset",
    )
    non_topmed_AN_nfe_onf: int = Field(
        default=None,
        description="Total number of alleles in samples of non-Finnish but otherwise indeterminate European ancestry in the non_topmed subset",
    )
    non_topmed_AN_nfe_seu: int = Field(
        default=None,
        description="Total number of alleles in samples of Southern European ancestry in the non_topmed subset",
    )
    non_topmed_AN_nfe_swe: int = Field(
        default=None,
        description="Total number of alleles in samples of Swedish ancestry in the non_topmed subset",
    )
    non_topmed_AN_oth: int = Field(
        default=None,
        description="Total number of alleles in samples of uncertain ancestry in the non_topmed subset",
    )
    non_topmed_AN_oth_female: int = Field(
        default=None,
        description="Total number of alleles in female samples of uncertain ancestry in the non_topmed subset",
    )
    non_topmed_AN_oth_male: int = Field(
        default=None,
        description="Total number of alleles in male samples of uncertain ancestry in the non_topmed subset",
    )
    non_topmed_AN_popmax: int = Field(
        default=None,
        description="Total number of alleles in the population with the maximum AF in the non_topmed subset",
    )
    non_topmed_AN_raw: int = Field(
        default=None,
        description="Total number of alleles in samples in the non_topmed subset, before removing low-confidence genotypes",
    )
    non_topmed_AN_sas: int = Field(
        default=None,
        description="Total number of alleles in samples of South Asian ancestry in the non_topmed subset",
    )
    non_topmed_AN_sas_female: int = Field(
        default=None,
        description="Total number of alleles in female samples of South Asian ancestry in the non_topmed subset",
    )
    non_topmed_AN_sas_male: int = Field(
        default=None,
        description="Total number of alleles in male samples of South Asian ancestry in the non_topmed subset",
    )
    non_topmed_faf95: float = Field(
        default=None,
        description="Filtering allele frequency (using Poisson 95% CI) for samples in the non_topmed subset",
    )
    non_topmed_faf95_afr: float = Field(
        default=None,
        description="Filtering allele frequency (using Poisson 95% CI) for samples of African-American ancestry in the non_topmed subset",
    )
    non_topmed_faf95_amr: float = Field(
        default=None,
        description="Filtering allele frequency (using Poisson 95% CI) for samples of Latino ancestry in the non_topmed subset",
    )
    non_topmed_faf95_eas: float = Field(
        default=None,
        description="Filtering allele frequency (using Poisson 95% CI) for samples of East Asian ancestry in the non_topmed subset",
    )
    non_topmed_faf95_nfe: float = Field(
        default=None,
        description="Filtering allele frequency (using Poisson 95% CI) for samples of non-Finnish European ancestry in the non_topmed subset",
    )
    non_topmed_faf95_sas: float = Field(
        default=None,
        description="Filtering allele frequency (using Poisson 95% CI) for samples of South Asian ancestry in the non_topmed subset",
    )
    non_topmed_faf99: float = Field(
        default=None,
        description="Filtering allele frequency (using Poisson 99% CI) for samples in the non_topmed subset",
    )
    non_topmed_faf99_afr: float = Field(
        default=None,
        description="Filtering allele frequency (using Poisson 99% CI) for samples of African-American ancestry in the non_topmed subset",
    )
    non_topmed_faf99_amr: float = Field(
        default=None,
        description="Filtering allele frequency (using Poisson 99% CI) for samples of Latino ancestry in the non_topmed subset",
    )
    non_topmed_faf99_eas: float = Field(
        default=None,
        description="Filtering allele frequency (using Poisson 99% CI) for samples of East Asian ancestry in the non_topmed subset",
    )
    non_topmed_faf99_nfe: float = Field(
        default=None,
        description="Filtering allele frequency (using Poisson 99% CI) for samples of non-Finnish European ancestry in the non_topmed subset",
    )
    non_topmed_faf99_sas: float = Field(
        default=None,
        description="Filtering allele frequency (using Poisson 99% CI) for samples of South Asian ancestry in the non_topmed subset",
    )
    non_topmed_nhomalt: int = Field(
        default=None,
        description="Count of homozygous individuals in samples in the non_topmed subset",
    )
    non_topmed_nhomalt_afr: int = Field(
        default=None,
        description="Count of homozygous individuals in samples of African-American ancestry in the non_topmed subset",
    )
    non_topmed_nhomalt_afr_female: int = Field(
        default=None,
        description="Count of homozygous individuals in female samples of African-American ancestry in the non_topmed subset",
    )
    non_topmed_nhomalt_afr_male: int = Field(
        default=None,
        description="Count of homozygous individuals in male samples of African-American ancestry in the non_topmed subset",
    )
    non_topmed_nhomalt_amr: int = Field(
        default=None,
        description="Count of homozygous individuals in samples of Latino ancestry in the non_topmed subset",
    )
    non_topmed_nhomalt_amr_female: int = Field(
        default=None,
        description="Count of homozygous individuals in female samples of Latino ancestry in the non_topmed subset",
    )
    non_topmed_nhomalt_amr_male: int = Field(
        default=None,
        description="Count of homozygous individuals in male samples of Latino ancestry in the non_topmed subset",
    )
    non_topmed_nhomalt_asj: int = Field(
        default=None,
        description="Count of homozygous individuals in samples of Ashkenazi Jewish ancestry in the non_topmed subset",
    )
    non_topmed_nhomalt_asj_female: int = Field(
        default=None,
        description="Count of homozygous individuals in female samples of Ashkenazi Jewish ancestry in the non_topmed subset",
    )
    non_topmed_nhomalt_asj_male: int = Field(
        default=None,
        description="Count of homozygous individuals in male samples of Ashkenazi Jewish ancestry in the non_topmed subset",
    )
    non_topmed_nhomalt_eas: int = Field(
        default=None,
        description="Count of homozygous individuals in samples of East Asian ancestry in the non_topmed subset",
    )
    non_topmed_nhomalt_eas_female: int = Field(
        default=None,
        description="Count of homozygous individuals in female samples of East Asian ancestry in the non_topmed subset",
    )
    non_topmed_nhomalt_eas_jpn: int = Field(
        default=None,
        description="Count of homozygous individuals in samples of Japanese ancestry in the non_topmed subset",
    )
    non_topmed_nhomalt_eas_kor: int = Field(
        default=None,
        description="Count of homozygous individuals in samples of Korean ancestry in the non_topmed subset",
    )
    non_topmed_nhomalt_eas_male: int = Field(
        default=None,
        description="Count of homozygous individuals in male samples of East Asian ancestry in the non_topmed subset",
    )
    non_topmed_nhomalt_eas_oea: int = Field(
        default=None,
        description="Count of homozygous individuals in samples of non-Korean, non-Japanese East Asian ancestry in the non_topmed subset",
    )
    non_topmed_nhomalt_female: int = Field(
        default=None,
        description="Count of homozygous individuals in female samples in the non_topmed subset",
    )
    non_topmed_nhomalt_fin: int = Field(
        default=None,
        description="Count of homozygous individuals in samples of Finnish ancestry in the non_topmed subset",
    )
    non_topmed_nhomalt_fin_female: int = Field(
        default=None,
        description="Count of homozygous individuals in female samples of Finnish ancestry in the non_topmed subset",
    )
    non_topmed_nhomalt_fin_male: int = Field(
        default=None,
        description="Count of homozygous individuals in male samples of Finnish ancestry in the non_topmed subset",
    )
    non_topmed_nhomalt_male: int = Field(
        default=None,
        description="Count of homozygous individuals in male samples in the non_topmed subset",
    )
    non_topmed_nhomalt_nfe: int = Field(
        default=None,
        description="Count of homozygous individuals in samples of non-Finnish European ancestry in the non_topmed subset",
    )
    non_topmed_nhomalt_nfe_bgr: int = Field(
        default=None,
        description="Count of homozygous individuals in samples of Bulgarian ancestry in the non_topmed subset",
    )
    non_topmed_nhomalt_nfe_est: int = Field(
        default=None,
        description="Count of homozygous individuals in samples of Estonian ancestry in the non_topmed subset",
    )
    non_topmed_nhomalt_nfe_female: int = Field(
        default=None,
        description="Count of homozygous individuals in female samples of non-Finnish European ancestry in the non_topmed subset",
    )
    non_topmed_nhomalt_nfe_male: int = Field(
        default=None,
        description="Count of homozygous individuals in male samples of non-Finnish European ancestry in the non_topmed subset",
    )
    non_topmed_nhomalt_nfe_nwe: int = Field(
        default=None,
        description="Count of homozygous individuals in samples of North-Western European ancestry in the non_topmed subset",
    )
    non_topmed_nhomalt_nfe_onf: int = Field(
        default=None,
        description="Count of homozygous individuals in samples of non-Finnish but otherwise indeterminate European ancestry in the non_topmed subset",
    )
    non_topmed_nhomalt_nfe_seu: int = Field(
        default=None,
        description="Count of homozygous individuals in samples of Southern European ancestry in the non_topmed subset",
    )
    non_topmed_nhomalt_nfe_swe: int = Field(
        default=None,
        description="Count of homozygous individuals in samples of Swedish ancestry in the non_topmed subset",
    )
    non_topmed_nhomalt_oth: int = Field(
        default=None,
        description="Count of homozygous individuals in samples of uncertain ancestry in the non_topmed subset",
    )
    non_topmed_nhomalt_oth_female: int = Field(
        default=None,
        description="Count of homozygous individuals in female samples of uncertain ancestry in the non_topmed subset",
    )
    non_topmed_nhomalt_oth_male: int = Field(
        default=None,
        description="Count of homozygous individuals in male samples of uncertain ancestry in the non_topmed subset",
    )
    non_topmed_nhomalt_popmax: int = Field(
        default=None,
        description="Count of homozygous individuals in the population with the maximum allele frequency in the non_topmed subset",
    )
    non_topmed_nhomalt_raw: int = Field(
        default=None,
        description="Count of homozygous individuals in samples in the non_topmed subset, before removing low-confidence genotypes",
    )
    non_topmed_nhomalt_sas: int = Field(
        default=None,
        description="Count of homozygous individuals in samples of South Asian ancestry in the non_topmed subset",
    )
    non_topmed_nhomalt_sas_female: int = Field(
        default=None,
        description="Count of homozygous individuals in female samples of South Asian ancestry in the non_topmed subset",
    )
    non_topmed_nhomalt_sas_male: int = Field(
        default=None,
        description="Count of homozygous individuals in male samples of South Asian ancestry in the non_topmed subset",
    )
    non_topmed_popmax: str = Field(
        default=None, description="Population with maximum AF in the non_topmed subset"
    )
    nonpar: str = Field(
        default=None,
        description="Variant (on sex chromosome) falls outside a pseudoautosomal region",
    )
    pab_max: float = Field(
        default=None,
        description="Maximum p-value over callset for binomial test of observed allele balance for a heterozygous genotype, given expectation of AB=0.5",
    )
    popmax: str = Field(default=None, description="Population with maximum AF")
    rf_label: str = Field(default=None, description="Random forest training label")
    rf_negative_label: str = Field(
        default=None,
        description="Variant was labelled as a negative example for training of random forest model",
    )
    rf_positive_label: str = Field(
        default=None,
        description="Variant was labelled as a positive example for training of random forest model",
    )
    rf_tp_probability: float = Field(
        default=None,
        description="Random forest prediction probability for a site being a true variant",
    )
    rf_train: str = Field(
        default=None, description="Variant was used in training random forest model"
    )
    segdup: str = Field(
        default=None, description="Variant falls within a segmental duplication region"
    )
    transmitted_singleton: str = Field(
        default=None,
        description="Variant was a callset-wide doubleton that was transmitted within a family (i.e., a singleton amongst unrelated sampes in cohort)",
    )
    variant_type: str = Field(
        default=None,
        description="Variant type (snv, indel, multi-snv, multi-indel, or mixed)",
    )
    vep: str = Field(
        default=None,
        description="Consequence annotations from Ensembl VEP. Format: Allele|Consequence|IMPACT|SYMBOL|Gene|Feature_type|Feature|BIOTYPE|EXON|INTRON|HGVSc|HGVSp|cDNA_position|CDS_position|Protein_position|Amino_acids|Codons|Existing_variation|ALLELE_NUM|DISTANCE|STRAND|FLAGS|VARIANT_CLASS|MINIMISED|SYMBOL_SOURCE|HGNC_ID|CANONICAL|TSL|APPRIS|CCDS|ENSP|SWISSPROT|TREMBL|UNIPARC|GENE_PHENO|SIFT|PolyPhen|DOMAINS|HGVS_OFFSET|GMAF|AFR_MAF|AMR_MAF|EAS_MAF|EUR_MAF|SAS_MAF|AA_MAF|EA_MAF|ExAC_MAF|ExAC_Adj_MAF|ExAC_AFR_MAF|ExAC_AMR_MAF|ExAC_EAS_MAF|ExAC_FIN_MAF|ExAC_NFE_MAF|ExAC_OTH_MAF|ExAC_SAS_MAF|CLIN_SIG|SOMATIC|PHENO|PUBMED|MOTIF_NAME|MOTIF_POS|HIGH_INF_POS|MOTIF_SCORE_CHANGE|LoF|LoF_filter|LoF_flags|LoF_info",
    )
    was_mixed: str = Field(default=None, description="Variant type was mixed")


class GnomadVariantModel(VariantModel):
    infos: GnomadInfosModel
