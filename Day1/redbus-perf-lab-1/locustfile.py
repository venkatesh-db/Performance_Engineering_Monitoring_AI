"""
Baseline workload for Module 1 hands-on lab.

Run: locust -f locustfile.py --host=http://127.0.0.1:8000
Then open http://localhost:8089, set users/spawn-rate, and watch
RPS, response time percentiles (P50/P95/P99) and error rate live.
"""
import random
from locust import HttpUser, task, between

ROUTES = [
    ("Bangalore", "Chennai"),
    ("Bangalore", "Hyderabad"),
    ("Pune", "Mumbai"),
]


class BusPassenger(HttpUser):
    wait_time = between(1, 3)

    @task(3)
    def search(self):
        src, dst = random.choice(ROUTES)
        self.client.get(
            "/api/search",
            params={"source": src, "destination": dst, "travel_date": "2026-08-15"},
            name="/api/search",
        )

    @task(1)
    def search_then_pick_seats(self):
        src, dst = random.choice(ROUTES)
        r = self.client.get(
            "/api/search",
            params={"source": src, "destination": dst, "travel_date": "2026-08-15"},
            name="/api/search",
        )
        buses = r.json().get("buses", [])
        if buses:
            bus_id = random.choice(buses)["id"]
            self.client.get(f"/api/seats/{bus_id}", name="/api/seats/[id]")
