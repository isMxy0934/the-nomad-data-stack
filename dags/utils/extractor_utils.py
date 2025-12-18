from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True)
class ExtractorDagSpec:
    dag_id: str
    task_id: str
    schedule: str | None
    start_date: datetime
    tags: list[str]
    destination_key_template: str
    fetcher: str


@dataclass(frozen=True)
class CsvPayload:
    csv_bytes: bytes
    record_count: int
