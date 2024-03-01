from dataclasses import dataclass
from pathlib import Path
from typing import List
from typing import Literal
from typing import Optional
from typing import Union

import pandas as pd
from pandas import DataFrame
from sqlalchemy.engine import Engine


CSV_CHUNKSIZE = 10000


@dataclass
class IfExists:
    value: Literal["fail", "append", "replace"]

    @staticmethod
    def choices():
        return ["fail", "append", "replace"]


IF_EXISTS_FAIL = IfExists(value="fail")
IF_EXISTS_APPEND = IfExists(value="append")
IF_EXISTS_REPLACE = IfExists(value="replace")


def dump_csv_to_database(
    engine: Engine,
    csv_path: Path,
    schema: str,
    table: str,
    if_exists: IfExists = IF_EXISTS_REPLACE,
) -> None:
    """Dumps the csv at the supplied path to the specified table."""

    print(f"Reading the csv at path: {str(csv_path)}")
    df = pd.read_csv(csv_path)

    print(f"Writing the csv data to table: '{schema}.{table}'.")
    df.to_sql(
        con=engine,
        name=table,
        schema=schema,
        chunksize=CSV_CHUNKSIZE,
        method="multi",
        index=False,
        if_exists=if_exists.value,
    )

    print(f"Completed. Data is ready in the table: '{schema}.{table}'.")


def dump_query_results_to_csv(
    engine: Engine,
    query: str,
    path: Union[str, Path],
    page_size: Optional[int] = None,
    exclude_fields: List[str] = [],
):
    if not page_size:
        # Dump the whole table to a single csv
        print(f"Executing query...")
        df = pd.read_sql(con=engine, sql=query)
        _dump_df_to_csv(df=df, path=path, exclude_fields=exclude_fields)
        print(f"Dumped query results to path {path}")
    else:
        # Dump the table in paginated csvs using the chunksize argument
        for page, df in enumerate(
            pd.read_sql(con=engine, sql=query, chunksize=page_size),
        ):
            # Add a page number suffix to the file name
            paginated_path = str(path).replace('.csv', f'_page_{page + 1}.csv')
            _dump_df_to_csv(
                df=df,
                path=paginated_path,
                exclude_fields=exclude_fields,
            )
            print(
                f"Dumped table query results page {page + 1} "
                f"to path {paginated_path}"
            )


def dump_database_table_to_csv(
    engine: Engine, 
    schema: str, 
    table: str,
    path: Union[str, Path],
    page_size: Optional[int] = None,
    exclude_fields: List[str] = [],
) -> None:
    """Dumps a database table to a csv file at the supplied path."""

    query = f"select * from {schema}.{table}"
    dump_query_results_to_csv(
        engine=engine,
        query=query,
        path=path,
        page_size=page_size,
        exclude_fields=exclude_fields,
    )


def _dump_df_to_csv(
    df: DataFrame,
    path: Union[str, Path],
    exclude_fields: List[str] = [],
):
    """Dumps the supplied dataframe to a csv at the supplied path."""

    if len(exclude_fields) > 0:
        df.drop(labels=exclude_fields, axis=1, inplace=True)

    df.to_csv(path_or_buf=path, index=False)