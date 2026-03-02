"""
Abstract vocabulary provider interface and OMOP implementation.

Vocabulary providers resolve concept identifiers to human-readable codes.
The ``$vocab:@column`` DSL syntax dispatches to the provider registered
under ``vocab``.  In practice only the OMOP provider is used today, but the
abstract interface allows future extension (e.g. FHIR, SNOMED, custom
ontologies).
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

import polars as pl


class VocabularyProvider(ABC):
    """Base class for vocabulary providers.

    Subclasses supply a concept DataFrame and know how to resolve a concept
    identifier column to a code string via a DataFrame join.
    """

    @property
    @abstractmethod
    def name(self) -> str:
        """Short identifier used in the ``$vocab:`` DSL prefix (e.g. ``omop``)."""
        ...

    @abstractmethod
    def get_concept_df(self) -> pl.DataFrame:
        """Return the concept mapping DataFrame.

        Must contain at least ``concept_id`` (Int64) and ``code`` (Utf8).
        """
        ...

    @abstractmethod
    def build_join(
        self,
        df: pl.DataFrame,
        concept_id_col: str,
        output_alias: str,
        concept_field: str = "code",
    ) -> pl.DataFrame:
        """Join *df* against the concept table, adding *output_alias*.

        Parameters
        ----------
        df:
            Source DataFrame.
        concept_id_col:
            Column in *df* that contains concept identifiers.
        output_alias:
            Name of the output column that will hold the resolved code.
        concept_field:
            Which concept attribute to return (default ``"code"``).
        """
        ...

    def filter_to_used_ids(self, used_ids: Set[int]) -> None:  # noqa: B027
        """Optionally trim the internal concept table to *used_ids*."""


class OMOPVocabularyProvider(VocabularyProvider):
    """OMOP CDM vocabulary provider backed by a ``concept`` Parquet table."""

    def __init__(self, concept_df: pl.DataFrame) -> None:
        self._concept_df = concept_df

    @property
    def name(self) -> str:
        return "omop"

    def get_concept_df(self) -> pl.DataFrame:
        return self._concept_df

    def build_join(
        self,
        df: pl.DataFrame,
        concept_id_col: str,
        output_alias: str,
        concept_field: str = "code",
    ) -> pl.DataFrame:
        if concept_field not in self._concept_df.columns:
            raise ValueError(
                f"Concept field '{concept_field}' not found in concept DataFrame. "
                f"Available: {self._concept_df.columns}"
            )
        join_df = self._concept_df.select(
            [
                pl.col("concept_id"),
                pl.col(concept_field).alias(output_alias),
            ]
        )
        result = df.join(join_df, left_on=concept_id_col, right_on="concept_id", how="left")
        if "concept_id" in result.columns and concept_id_col != "concept_id":
            result = result.drop("concept_id")
        return result

    def filter_to_used_ids(self, used_ids: Set[int]) -> None:
        if used_ids:
            self._concept_df = self._concept_df.filter(pl.col("concept_id").is_in(list(used_ids)))

    @classmethod
    def from_omop_dir(cls, omop_dir: Path, verbose: bool = False) -> Tuple["OMOPVocabularyProvider", Dict[int, Any]]:
        """Build provider from an OMOP data directory.

        Returns ``(provider, code_metadata)`` where *code_metadata* contains
        custom concept information for downstream use.
        """
        from meds_etl.omop_common import build_concept_map

        concept_df, code_metadata = build_concept_map(omop_dir, verbose=verbose)
        return cls(concept_df), code_metadata


class VocabularyRegistry:
    """Registry mapping vocabulary names to providers."""

    def __init__(self) -> None:
        self._providers: Dict[str, VocabularyProvider] = {}

    def register(self, provider: VocabularyProvider) -> None:
        self._providers[provider.name] = provider

    def get(self, name: str) -> Optional[VocabularyProvider]:
        return self._providers.get(name)

    def __contains__(self, name: str) -> bool:
        return name in self._providers

    @property
    def names(self) -> List[str]:
        return list(self._providers.keys())
