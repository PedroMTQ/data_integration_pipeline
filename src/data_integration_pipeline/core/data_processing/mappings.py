from functools import cached_property
from data_integration_pipeline.settings import SRC_DATA
import polars as pl
import os
from data_integration_pipeline.core.singletons_base import SingletonBase
from rapidfuzz import process, utils
from data_integration_pipeline.io.logger import logger


class MappingsSingleton(SingletonBase):
    def __str__(self):
        # Check __dict__ to see if the property has been 'materialized'
        if "_mapping" in self.__dict__:
            return f"{self.__class__.__name__} - {len(self._mapping)} entries"
        return f"{self.__class__.__name__} - Data not yet loaded)"

    def get_label(self, code: str | None) -> str | None:
        """Safe lookup for your Pydantic computed fields."""
        if not code:
            return None
        return self._mapping.get(str(code))

    @cached_property
    def _reverse_mapping(self) -> dict:
        """Inverts the mapping to {description: code} for fuzzy lookups."""
        # We ensure we only map non-None values to avoid key errors
        return {str(v): k for k, v in self._mapping.items() if v is not None}

    # ! this is obviosly not the best way to do this, but it works for a POC
    # if this was better, you could e.g., use it as a blocking rule during entity resolution
    def get_code(self, label: str | None, threshold: int = 80) -> str | None:
        """Uses rapidfuzz to find the best matching code for a given label."""
        if not label:
            return None
        # extractOne returns: (value, score, index)
        match = process.extractOne(
            query=str(label),
            choices=self._reverse_mapping.keys(),
            processor=utils.default_process,
            score_cutoff=threshold,
        )
        if match:
            # match[0] is the 'description' that won
            code = self._reverse_mapping.get(match[0])
            logger.debug(f'Found match for "{label}": code: {code} | label: "{match[0]}" | score: {match[1]}')
            return code
        return None


class NaicsMapping(MappingsSingleton):
    @cached_property
    def _mapping(self) -> dict:
        df = pl.read_csv(
            os.path.join(SRC_DATA, "naics_mapping.csv"),
            schema_overrides={"NAICS code": pl.String, "NAICS description": pl.String},
        )
        return dict(zip(df["NAICS code"], df["NAICS description"], strict=True))


if __name__ == "__main__":
    naics_mapping = NaicsMapping()
    print("1", id(naics_mapping), naics_mapping)
    print(naics_mapping.get_label("561611"))
    print("2", id(naics_mapping), naics_mapping)
    naics_mapping = NaicsMapping()
    print("3", id(naics_mapping), naics_mapping)
    print("code lookup", naics_mapping.get_code("Structural Stel and precast Concryt Contractors"))
