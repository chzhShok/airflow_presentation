from typing import Any, Dict, Optional
import asyncio
import psycopg2

from airflow.triggers.base import BaseTrigger, TriggerEvent
from airflow.models import BaseOperator
from airflow.utils.context import Context
from airflow.providers.postgres.hooks.postgres import PostgresHook


class SQLCheckTrigger(BaseTrigger):
    def __init__(self, conn_info: Dict[str, Any], sql: str, poll_interval: int = 10):
        super().__init__()
        self.conn_info = conn_info
        self.sql = sql
        self.poll_interval = poll_interval

    def serialize(self) -> tuple[str, Dict[str, Any]]:
        return (
            f"{self.__class__.__module__}.{self.__class__.__name__}",
            {"conn_info": self.conn_info, "sql": self.sql, "poll_interval": self.poll_interval},
        )

    async def run(self):
        while True:
            try:
                def do_check() -> Optional[int]:
                    conn_args = {}
                    if self.conn_info.get("host"):
                        conn_args["host"] = self.conn_info["host"]
                    if self.conn_info.get("port"):
                        conn_args["port"] = int(self.conn_info["port"])
                    if self.conn_info.get("user"):
                        conn_args["user"] = self.conn_info["user"]
                    if self.conn_info.get("password"):
                        conn_args["password"] = self.conn_info["password"]
                    dbname = self.conn_info.get("schema") or self.conn_info.get("database") or self.conn_info.get("dbname")
                    if dbname:
                        conn_args["dbname"] = dbname

                    conn = psycopg2.connect(**conn_args)
                    try:
                        with conn.cursor() as cur:
                            cur.execute(self.sql)
                            res = cur.fetchone()
                            if res and len(res) > 0:
                                try:
                                    return int(res[0])
                                except Exception:
                                    return None
                            return None
                    finally:
                        conn.close()

                result = await asyncio.to_thread(do_check)

                if result and int(result) > 0:
                    yield TriggerEvent({"status": "success", "rows": int(result)})
                    return

            except Exception as exc:
                yield TriggerEvent({"status": "error", "message": str(exc)})
                return

            await asyncio.sleep(self.poll_interval)


class SQLCheckOperator(BaseOperator):
    def __init__(self, conn_id: str, sql: str, poll_interval: int = 10, **kwargs):
        super().__init__(**kwargs)
        self.conn_id = conn_id
        self.sql = sql
        self.poll_interval = poll_interval

    def _resolve_conn_info(self) -> Dict[str, Any]:
        hook = PostgresHook(postgres_conn_id=self.conn_id)
        conn = hook.get_connection(self.conn_id)

        conn_info: Dict[str, Any] = {
            "host": conn.host,
            "port": conn.port,
            "schema": conn.schema,
            "user": conn.login,
            "password": conn.password,
        }
        
        return {k: v for k, v in conn_info.items() if v is not None}

    def execute(self, context: Context):
        conn_info = self._resolve_conn_info()

        self.defer(
            trigger=SQLCheckTrigger(conn_info=conn_info, sql=self.sql, poll_interval=self.poll_interval),
            method_name="execute_complete",
        )

    def execute_complete(self, context: Context, event: Dict[str, Any]):
        status = event.get("status")
        if status == "success":
            rows = event.get("rows", 0)
            return rows
        else:
            raise RuntimeError(f"SQLCheckOperator failed: {event}")
