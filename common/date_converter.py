from datetime import datetime, date, timedelta
from typing import Union


def get_date_formats(
    value: Union[str, datetime, date],
    offset: int = 0,
) -> dict:
   
    if value is None:
        raise ValueError("Date value cannot be None")

    if isinstance(value, datetime):
        dt = value
    elif isinstance(value, date):
        dt = datetime.combine(value, datetime.min.time())
    elif isinstance(value, str):
        value = value.strip()
        formats = [
            "%Y-%m-%d", "%Y/%m/%d", "%d-%m-%Y", "%d/%m/%Y",
            "%m-%d-%Y", "%m/%d/%Y", "%Y%m%d",
            "%d-%b-%Y", "%d-%b-%y", "%d %b %Y", "%d %B %Y",
            "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M:%S.%f",
            "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%S.%f",
        ]
        for fmt in formats:
            try:
                dt = datetime.strptime(value, fmt)
                break
            except ValueError:
                continue
        else:
            raise ValueError(f"Unsupported date format: {value}")
    else:
        raise TypeError(f"Unsupported type: {type(value).__name__}")

    
    dt = dt + timedelta(days=offset)

    return {
        "date_string": dt.strftime("%Y-%m-%d"),
        "date_file_name": dt.strftime("%d%m%Y"),
        "oracle_string": dt.strftime("%d-%b-%y").upper(),
        "day": dt.strftime("%d"),
        "month": dt.strftime("%b").upper(),
        "year": dt.strftime("%Y"),
    }   