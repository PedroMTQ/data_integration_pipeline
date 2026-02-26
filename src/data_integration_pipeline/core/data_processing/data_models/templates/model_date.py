import os
from typing import Any, Optional

import dateparser
from pydantic import BaseModel, Field, model_validator

DAY_DATE_FORMAT = os.getenv("FULL_DATE_FORMAT", "%Y-%m-%d")
MONTH_DATE_FORMAT = os.getenv("FULL_DATE_FORMAT", "%Y-%m")
YEAR_DATE_FORMAT = os.getenv("FULL_DATE_FORMAT", "%Y")
DATE_PARSER = dateparser.DateDataParser()


class ModelDate(BaseModel):
    year: int = Field(description="Year", ge=1500)
    month: Optional[int] = Field(default=None, description="Month of the year", ge=1, le=12)
    day: Optional[int] = Field(default=None, description="Day of the month", ge=1, le=31)

    @model_validator(mode="before")
    @classmethod
    def parse_string_date(cls, value: Any) -> Any:
        if isinstance(value, (dict, cls)):
            return value
        if value is None:
            return None
        if isinstance(value, int) and len(str(value)) == 4:
            return {"year": value, "month": None, "day": None}
        if isinstance(value, str):
            if value.isdigit() and len(value) == 4:
                return {"year": int(value), "month": None, "day": None}
        try:
            parsed_result = DATE_PARSER.get_date_data(value)
            if parsed_result and hasattr(parsed_result, "date_obj") and parsed_result.date_obj:
                date = parsed_result.date_obj
                return {"year": date.year, "month": date.month, "day": date.day}
            raise ValueError(f"Parser returned empty date for: {value}")
        except Exception as e:
            raise ValueError(f"Could not parse date string '{value}': {str(e)}") from e


if __name__ == "__main__":
    test_dates = [
        "2023-08-15",
        "2023-08",
        "2023",
        "15th August 2023",
        "August 15, 2023",
        "2023/08/15",
        "2023.08.15",
        "15-08-2023",
        "Invalid Date String",
        None,
        2023,
    ]

    for date_str in test_dates:
        try:
            model_date = ModelDate.model_validate(date_str)
            print(f"Input: {date_str} => Parsed: {model_date} => Serialized: {repr(model_date.model_dump())}")
        except ValueError as ve:
            print(f"Input: {date_str} => Error: {ve}")
