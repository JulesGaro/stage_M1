import logging

from csv import DictReader
from io import TextIOWrapper
from pydantic import ValidationError
from typing import Generator, NewType, Union

from diagho_toolkit.models import (
    GnomadConstraintGeneModel,
    GnomadConstraintAttributesModel,
)

logger = logging.getLogger(__name__)


GnomadIOWrapper = NewType("GnomadIOWrapper", TextIOWrapper)


def gnomad_reader(gnomad_filename: str) -> GnomadIOWrapper:
    return GnomadIOWrapper(
        open(gnomad_filename, mode="r", encoding="utf_8", errors="strict")
    )


def gene_reader(
    gnomad_stream: GnomadIOWrapper,
) -> Generator[
    Union[tuple[GnomadConstraintGeneModel, None], tuple[None, ValidationError]],
    None,
    None,
]:
    gnomad_stream_reading = DictReader(gnomad_stream, delimiter="\t")
    for row in gnomad_stream_reading:

        model_args = {
            "symbol": row["gene"],
            "name": row["gene"],
            "gene_type": row["gene_type"],
            "aliases": [
                row["gene"],
                row["gene_type"],
                "{0}-{1}".format(str(row["start_position"]), str(row["end_position"])),
            ],
            "attributes": GnomadConstraintAttributesModel(**row).dict(),
        }

        try:
            yield GnomadConstraintGeneModel(**model_args), None
        except ValidationError as e:
            yield None, e


def get_genes(gnomad_filename: str) -> Generator[GnomadConstraintGeneModel, None, None]:
    with gnomad_reader(gnomad_filename=gnomad_filename) as gnomad_stream:
        for gene, error in gene_reader(
            gnomad_stream=gnomad_stream,
        ):
            if error:
                logger.error(error.json())
            else:
                yield gene
