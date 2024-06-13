import psycopg
# from airflow.hooks.base import BaseHook


class PgConnect:
    def __init__(self, host: str, port: str, dbname: str, user: str, pw: str, sslmode: str = "require") -> None:
        self.host = host
        self.port = int(port)
        self.dbname = dbname
        self.user = user
        self.pw = pw
        self.sslmode = sslmode

    def url(self) -> str:
        return """
            host={host}
            port={port}
            dbname={dbname}
            user={user}
            password={pw}
            target_session_attrs=read-write
            sslmode={sslmode}
        """.format(
            host=self.host,
            port=self.port,
            dbname=self.dbname,
            user=self.user,
            pw=self.pw,
            sslmode=self.sslmode)

    def client(self):
        return psycopg.connect(self.url())
