import vertica_python

class VertConnect():
    def __init__(self, host: str, port: str, user: str, pw: str) -> None:
        self.host = host
        self.port = int(port)
        self.user = user
        self.pw = pw
    
    def connect(self):
        vertica_conn = vertica_python.connect(
            host=self.host,
            port=self.port,
            user=self.user,
            password = self.pw
            )
        return vertica_conn