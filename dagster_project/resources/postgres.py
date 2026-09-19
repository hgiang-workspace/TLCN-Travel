"""PostgreSQL resource - metadata storage for Dagster and application data."""

from dagster import ConfigurableResource
from pydantic import Field
from typing import Optional
import psycopg2
import logging

logger = logging.getLogger(__name__)


class PostgresResource(ConfigurableResource):
    """Dagster resource for PostgreSQL connections."""

    host: str = Field(default="postgres")
    port: int = Field(default=5432)
    database: str = Field(default="tourism")
    user: str = Field(default="tourism")
    password: str = Field(default="tourism")

    def get_connection(self):
        """Create a psycopg2 connection."""
        return psycopg2.connect(
            host=self.host,
            port=self.port,
            database=self.database,
            user=self.user,
            password=self.password,
        )

    def execute_query(self, query: str, params: tuple | None = None) -> list[dict]:
        """Execute a query and return results as list of dicts."""
        conn = self.get_connection()
        try:
            cur = conn.cursor()
            cur.execute(query, params)
            columns = [desc[0] for desc in cur.description] if cur.description else []
            rows = cur.fetchall()
            return [dict(zip(columns, row)) for row in rows]
        finally:
            conn.close()

    def execute_statement(self, query: str, params: tuple | None = None) -> int:
        """Execute a statement (INSERT/UPDATE/DELETE) and return rowcount."""
        conn = self.get_connection()
        try:
            cur = conn.cursor()
            cur.execute(query, params)
            conn.commit()
            return cur.rowcount
        finally:
            conn.close()


postgres_resource = PostgresResource()
