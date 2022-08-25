import logging

from celery import chain
from pathlib import Path
from typing import List, Dict, Any

from django.core.exceptions import ObjectDoesNotExist
from django.db import models
from django.conf import settings
from django.utils.translation import gettext_lazy

from assemblies.models import Assembly, Region
from factories.models import Factory, NodeVariantAnnotation
from core.models.mixins import MetadataMixin
from core.tasks import (
    factory_run,
    factory_set_loading_success,
    factory_set_loading_started,
)
from diagho_toolkit.models import GnomadVariantModel, GnomadInfosModel
from diagho_toolkit.vcf import reader
from gnomad_factories.catalog import GNOMAD_CATALOG
from variants.models import Variant, VariantAnnotation

logger = logging.getLogger(__name__)


class GnomadFactory(Factory, MetadataMixin):

    assembly = models.ForeignKey(
        Assembly,
        on_delete=models.CASCADE,
        related_name="gnomad_factories",
        related_query_name="gnomad_factory",
        verbose_name=gettext_lazy("assembly"),
    )

    _gnomad_vcf_file_path = models.TextField(blank=True)

    @property
    def gnomad_vcf_file_path(self) -> Path:
        return Path(self._gnomad_vcf_file_path)

    @gnomad_vcf_file_path.setter
    def gnomad_vcf_file_path(self, path: str) -> None:
        if not isinstance(path, Path):
            raise ValueError
        else:
            self._gnomad_vcf_file_path = str(path)

    @classmethod
    def load_catalog(cls, catalog=GNOMAD_CATALOG):

        for blueprint in catalog["gnomad_factories"]:
            try:
                gnomad_factory = cls.objects.get(name=blueprint["name"])
                created = False
            except ObjectDoesNotExist as e:
                logger.info(e)
                assembly, created = Assembly.objects.get_or_create(
                    id=blueprint["assembly"],
                )
                gnomad_factory = cls(
                    name=blueprint["name"],
                    description=blueprint["description"],
                    assembly=assembly,
                    _gnomad_vcf_file_path=settings.BIODB_ROOT
                    / blueprint["gnomad_vcf_file_path"],
                )
                created = True
                gnomad_factory.save()

            if created:
                logger.info("New gnomad: %s", gnomad_factory)
            else:
                logger.info("Found existing gnomad: %s", gnomad_factory)

    class Meta(MetadataMixin.Meta):
        verbose_name = gettext_lazy("gnomad factory")
        verbose_name_plural = gettext_lazy("gnomad factories")

    def load(self) -> None:
        chain(
            factory_set_loading_started.si(self.pk),
            factory_run.si(self.pk, function_name="load_regions"),
            factory_run.si(self.pk, function_name="load_variants"),
            factory_set_loading_success.si(self.pk),
        )()

    def run(self, function_name: str, kwargs) -> None:

        for f in [self.load_regions, self.load_variants, self.load_variants_for_region]:
            if f.__name__ == function_name:
                f(**kwargs)

    def load_regions(self) -> None:
        logger.info("Start loading Regions")
        for region in reader.get_contigs(self.gnomad_vcf_file_path):

            try:
                region_object = Region.objects.get(assembly=self.assembly, seqid=region)

            except ObjectDoesNotExist:
                region_object = Region(
                    assembly=self.assembly, seqid=region, factory=self
                )
                region_object.save()

        logger.info("Finish loading Regions")

    def load_variants(self) -> None:
        logger.info("Start loading Variants and VariantsAnnotation")
        for region in reader.get_contigs(self.gnomad_vcf_file_path):
            factory_run.delay(
                self.pk,
                function_name="load_variants_for_region",
                kwargs={"region": region},
            )

    def load_variants_for_region(self, region) -> None:
        logger.info(
            "Start loading Variant and VariantAnnotation for {0}".format(region)
        )
        region_obj = Region.objects.get(
            assembly=self.assembly,
            seqid=region,
        )
        for vcf_variant in reader.get_variants(
            vcf_filename=self.gnomad_vcf_file_path, contig=region
        ):

            gnomad_variant = GnomadVariantModel(
                chrom=vcf_variant.chrom,
                pos=vcf_variant.pos,
                ids=vcf_variant.ids,
                ref=vcf_variant.ref,
                alt=vcf_variant.alt,
                qual=vcf_variant.qual,
                filters=vcf_variant.filters,
                infos=GnomadInfosModel(**vcf_variant.infos),
            )

            variant, _ = Variant.objects.get_or_create(
                region=region_obj,
                pos=gnomad_variant.pos,
                ref=gnomad_variant.ref,
                alt=gnomad_variant.alt,
            )

            VariantAnnotation.objects.get_or_create(
                factory=self, variant=variant, metadata=gnomad_variant.infos.dict()
            )

        logger.info(
            "Finish loading Variant and VariantAnnotation for {0}".format(region)
        )

    def get_schema(self) -> Dict[str, Any]:
        return {str(self.pk): {"genes": GnomadInfosModel.schema()}}

    def init_metadata(
        self, node_variant_annotations: List[NodeVariantAnnotation]
    ) -> None:
        for node_variant_annotation in node_variant_annotations:
            # On récupère...

            # ..La variant_annotation correspondant variant
            try:
                variant_annotation = VariantAnnotation.objects.get(
                    factory=self, variant=node_variant_annotation.variant
                ).metadata

            except ObjectDoesNotExist:
                variant_annotation = {}

            # Et on l'ajoute aux metadatas
            node_variant_annotation.metadata = {
                "factories": {
                    str(self.pk): {"variant_annotations": [variant_annotation]},
                },
            }

            node_variant_annotation.building = NodeVariantAnnotation.BUILDING_SUCCESS
            node_variant_annotation.save()

    def merge_metadata(
        self,
        node_variant_annotation: NodeVariantAnnotation,
        parent_node_variant_annotation: NodeVariantAnnotation,
    ) -> None:

        # on récupère les metadata du/des parent.s
        metadata = parent_node_variant_annotation.metadata
        # On récupère...

        # ..La variant_annotation correspondant variant
        try:
            variant_annotation = VariantAnnotation.objects.get(
                factory=self, variant=node_variant_annotation.variant
            ).metadata

        except ObjectDoesNotExist:
            variant_annotation = {}

        metadata["factories"][str(self.pk)] = {
            "variant_annotations": [variant_annotation]
        }

        # On passe les metadata mise à jour au node_variant_annotation actuel
        node_variant_annotation.metadata = metadata

        node_variant_annotation.building = NodeVariantAnnotation.BUILDING_SUCCESS
        node_variant_annotation.save()
