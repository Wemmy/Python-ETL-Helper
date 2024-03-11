import argparse
from pathlib import Path
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

from utils.utils_redshift import RedshiftCredentials
from utils.utils_snowflake import SnowflakeCredentials
from utils.utils_csv import dump_csv_to_database
from utils.utils_csv import IfExists
from utils.utils_csv import IF_EXISTS_REPLACE



def main():
    """Gets the csv path and table destination using argparse, and writes the
        data from the specified csv to the specified table.
    """

    args = _handle_argparse()

    if args.redshift_or_snowflake == "redshift":
        engine = _make_redshift_engine(
            credentials_prefix=args.credentials_prefix,
        )
        
    elif args.redshift_or_snowflake == "snowflake":
        engine = _make_snowflake_engine(
            credentials_prefix=args.credentials_prefix,
            use_sso=args.use_sso,
        )

    else:
        raise Exception(
            f"Invalid database '{args.database}', "
            "must be one of 'redshift' or 'snowflake'"
        )

    dump_csv_to_database(
        engine=engine,
        csv_path=Path(args.csv_path),
        table=args.table,
        schema=args.schema,
        if_exists=IfExists(value=args.if_exists),
    )


def _handle_argparse():
    """Store arguments passed through argparse."""

    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--csv-path",
        help="The path to the csv that should be written to redshift.",
        required=True,
    )
    parser.add_argument(
        "--table",
        help="The table that the csv data should be written to.",
        required=True,
    )
    parser.add_argument(
        "--schema",
        help="The schema that the csv data should be written to.",
        required=True,
    )
    parser.add_argument(
        "--redshift-or-snowflake",
        help="redshift or snowflake",
        required=True,
    )
    parser.add_argument(
        "--credentials-prefix",
        help="The prefix added to your env vars for database credentials.",
        required=True,
    )
    parser.add_argument(
        "--use-sso",
        help="For snowflake only, to use SSO in an external browser.",
        action="store_true",
        required=False,
        default=False,
    )
    parser.add_argument(
        "--if-exists",
        help="What to do if the table already exists; fail, replace or append.",
        required=False,
        choices=IfExists.choices(),
        default=IF_EXISTS_REPLACE.value,
    )
    
    return parser.parse_args()


def _make_redshift_engine(credentials_prefix: str) -> Engine:
    """Creates an authenticated connection engine to redshift."""

    creds = RedshiftCredentials.from_env_vars(prefix=credentials_prefix)
    return create_engine(creds.url)


def _make_snowflake_engine(credentials_prefix: str, use_sso: bool = False) -> Engine:
    """Creates an authenticated connection engine to redshift."""

    creds = SnowflakeCredentials.from_env_vars(prefix=credentials_prefix)

    engine_kwargs = {"url": creds.url}

    if use_sso:
        engine_kwargs["connect_args"] = {
            'authenticator': 'externalbrowser',
        }

    return create_engine(**engine_kwargs)


if __name__ == "__main__":
    main()