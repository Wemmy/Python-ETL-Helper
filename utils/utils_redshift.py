import os
from dataclasses import dataclass
from utils.dataclasses import iterate_as_dict


@iterate_as_dict
@dataclass(frozen=True)
class RedshiftCredentials:
    username: str
    password: str
    host: str
    port: int
    db_name: str
    driver: str = "postgresql"

    @property
    def url(self):
        """Constructs the database url for the credentials instance."""

        return f"{self.driver}://{self.username}:{self.password}@{self.host}:{self.port}/{self.db_name}"

    @staticmethod
    def from_env_vars(prefix: str) -> "RedshiftCredentials":
        """Constructs a RedshiftCredentials instance from env vars.
        
        To use credentials from your environment variables, ensure your credentials all have
        the same prefix, and pass that prefix along through the "prefix" arg.

        For example, you can pass prefix="MY_PREFIX" and the following env vars will be used:
            - MY_PREFIX_USER
            - MY_PREFIX_ENV_SECRET_PASSWORD
            - MY_PREFIX_HOST
            - MY_PREFIX_PORT
            - MY_PREFIX_DB_NAME
        """

        return RedshiftCredentials(
            username=os.environ[f"{prefix}_USER"],
            password=os.environ[f"{prefix}_ENV_SECRET_PASSWORD"],
            host=os.environ[f"{prefix}_HOST"],
            port=os.environ[f"{prefix}_PORT"],
            db_name=os.environ[f"{prefix}_DB_NAME"],
        )

    @staticmethod
    def from_url(url: str) -> "RedshiftCredentials":
        """Constructs a RedshiftCredentials instance from the supplied database url."""

        driver, remaining_url = url.split("://", 1)
        username, remaining_url = remaining_url.split(":", 1)
        password, remaining_url = remaining_url.split("@", 1)
        host, remaining_url = remaining_url.split(":", 1)
        port, db_name = remaining_url.split("/", 1)

        return RedshiftCredentials(
            username=username,
            password=password,
            host=host,
            port=port,
            db_name=db_name,
            driver=driver,
        )
