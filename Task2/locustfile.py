from locust import HttpUser, between, task
import requests


class WebsiteUser(HttpUser):
    wait_time = between(1, 5)

    def on_start(self):
        # Отключаем connection pooling для честной L4 балансировки
        self.client.mount("http://", requests.adapters.HTTPAdapter(pool_connections=1, pool_maxsize=1))
        self.client.keep_alive = False

    @task
    def index(self):
        with self.client.get("/", headers={"Connection": "close"}, catch_response=True) as response:
            response.success()
