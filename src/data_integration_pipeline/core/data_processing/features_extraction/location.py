from __future__ import annotations
from typing import Literal, Optional
from dataclasses import dataclass, fields
from scourgify import normalize_address_record
from data_integration_pipeline.core.singletons_base import SingletonBase
from data_integration_pipeline.io.logger import logger


@dataclass
class ParsedLocation:
    address_1: Optional[str] = None
    address_2: Optional[str] = None
    postal_code: Optional[str] = None
    city: Optional[str] = None
    state: Optional[str] = None

    def __post_init__(self):
        for field in fields(self):
            field_name = field.name
            field_value = getattr(self, field_name)
            if isinstance(field_value, str):
                setattr(self, field_name, field_value.upper().strip())

    @classmethod
    def from_scourgify(cls, data: dict) -> "ParsedLocation":
        return cls(
            address_1=data.get("address_line_1"),
            address_2=data.get("address_line_2"),
            postal_code=data.get("postal_code"),
            city=data.get("city"),
            state=data.get("state"),
        )


class LocationParser(SingletonBase):
    def __init__(self, parser: Literal["scourgify"] = "scourgify"):
        if hasattr(self, "parser_type"):
            logger.debug(f"{self.__class__.__name__} singleton already initialized, skipping object creation...")
            return
        self.parser_type = parser
        # You'd probably add better parsers here, there are many trained models for this nowadays, but this is pretty lightweight for a POC
        if self.parser_type == "scourgify":
            self.parser = LocationParser.parser_scourgify

    @staticmethod
    def parser_scourgify(address: str) -> ParsedLocation:
        return ParsedLocation.from_scourgify(normalize_address_record(address, long_hand=True))

    def parse(self, address: str) -> ParsedLocation:
        try:
            return self.parser(address)
        except Exception as e:
            logger.error(f'Error parsing address "{address}" due to {e}')
            return ParsedLocation()


if __name__ == "__main__":
    test_address = "124 POWER AVE, SUITE B, alaska"
    parser = LocationParser()
    parsed_address = parser.parse(test_address)
    print(parsed_address)
    print("1", id(parser), parser.parser_type)
    parser = LocationParser(parser="test")
    print("2", id(parser), parser.parser_type)
