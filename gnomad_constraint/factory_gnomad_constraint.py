import logging

from celery import chain
from itertools import islice
from pathlib import Path
from typing import List, Dict, Any

from django.db import models
from django.conf import settings
from django.utils.translation import gettext_lazy

from factories.models import Factory, NodeVariantAnnotation
from core.models.mixins import MetadataMixin
from core.tasks import (
    factory_run,
    factory_set_loading_success,
    factory_set_loading_started,
)
from diagho_toolkit.gnomad_constraint import get_genes
from diagho_toolkit.models import (
    GnomadConstraintGeneModel,
)
from genomes.models import Gene
from gnomad_constraint_factories.catalog import GNOMAD_CONSTRAINT_CATALOG

logger = logging.getLogger(__name__)


class GnomadConstraintFactory(Factory, MetadataMixin):

    _gnomad_constraint_tsv_file_path = models.TextField(blank=True)

    @property
    def gnomad_constraint_tsv_file_path(self) -> Path:
        return Path(self._gnomad_constraint_tsv_file_path)

    @gnomad_constraint_tsv_file_path.setter
    def gnomad_constraint_tsv_file_path(self, path: Path) -> None:
        if not isinstance(path, Path):
            raise ValueError
        else:
            self._gnomad_constraint_tsv_file_path = str(path)

    @classmethod
    def load_catalog(cls, catalog=GNOMAD_CONSTRAINT_CATALOG):

        for blueprint in catalog["gnomad_constraint_factories"]:
            try:
                gnomad_constraint_factory = cls.objects.get(
                    name=blueprint["name"],
                )
                created = False
            except cls.DoesNotExist as e:
                logger.info(e)
                gnomad_constraint_factory = cls(
                    name=blueprint["name"],
                    description=blueprint["description"],
                    _gnomad_constraint_tsv_file_path=settings.BIODB_ROOT
                    / blueprint["gnomad_constraint_tsv_file_path"],
                )
                created = True
                gnomad_constraint_factory.save()

            if created:
                logger.info("New gnomad_constraint: %s", gnomad_constraint_factory)
            else:
                logger.info(
                    "Found existing gnomad_constraint: %s", gnomad_constraint_factory
                )

    class Meta(MetadataMixin.Meta):
        verbose_name = gettext_lazy("gnomad_constraint factory")
        verbose_name_plural = gettext_lazy("gnomad_constraint factories")

    def load(self) -> None:
        chain(
            factory_set_loading_started.si(self.pk),
            factory_run.si(
                self.pk, function_name="load_genes", kwargs={"batch_size": 100}
            ),
            factory_set_loading_success.si(self.pk),
        )()

    def run(self, function_name: str, kwargs) -> None:
        for f in [self.load_genes]:
            if f.__name__ == function_name:
                f(**kwargs)

    def load_genes(self, batch_size: int = 100):
        message = self.genomes_genes.all().delete()
        logger.info("Delete factory related objects: %s, %s", self, message)

        genes = get_genes(gnomad_filename=self.gnomad_constraint_tsv_file_path)
        while True:
            batch = list(islice(genes, batch_size))
            if not batch:
                break
            self.load_genes_by_batch(genes=batch)

    def load_genes_by_batch(self, genes: List[GnomadConstraintGeneModel] = []):
        Gene.objects.bulk_create(
            [
                Gene(
                    factory=self,
                    name=gene.name,
                    symbol=gene.symbol,
                    gene_type=gene.gene_type,
                    metadata=gene.attributes.dict(),
                )
                for gene in genes
            ]
        )

    def get_schema(self) -> Dict[str, Any]:
        return {str(self.pk): {"genes": GnomadConstraintGeneModel.schema()}}

    def init_metadata(
        self, node_variant_annotations: List[NodeVariantAnnotation]
    ) -> None:
        for node_variant_annotation in node_variant_annotations:
            # On récupère...

            # ..Les gènes dans lesquels tombe le variant

            # Et on les ajoutes au metadata
            variant = node_variant_annotation.variant
            node_variant_annotation.metadata = {
                "factories": {
                    str(self.pk): {
                        "genes": [
                            gene.metadata
                            for gene in Gene.objects.filter(
                                factory=self,
                                metadata__chromosome__in=variant.region.aliases,
                                metadata__start_position__lte=variant.pos
                                + len(variant.ref),
                                metadata__end_position__gte=variant.pos,
                            )
                        ],
                    },
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

        # ..Les gènes dans lesquels tombe le variant

        # Et on les ajoutes au metadata
        variant = node_variant_annotation.variant
        metadata["factories"][str(self.pk)] = {
            "genes": [
                gene.metadata
                for gene in Gene.objects.filter(
                    factory=self,
                    metadata__chromosome__in=variant.region.aliases,
                    metadata__start_position__lte=variant.pos + len(variant.ref),
                    metadata__end_position__gte=variant.pos,
                )
            ],
        }

        # On passe les metadata mise à jour au node_variant_annotation actuel
        node_variant_annotation.metadata = metadata

        node_variant_annotation.building = NodeVariantAnnotation.BUILDING_SUCCESS
        node_variant_annotation.save()
