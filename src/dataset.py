import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from typing import Literal


def generate_rides(n: int, day: str) -> pd.DataFrame:
    """Generate a synthetic rides dataset of size n for a given ISO date."""
    rng = np.random.default_rng(42)
    base = datetime.fromisoformat(day)
    ride_id = np.arange(1, n + 1, dtype=np.int64)
    user_id = rng.integers(1000, 9999, size=n)
    driver_id = rng.integers(100, 999, size=n)
    pickup_offset = rng.integers(0, 24 * 60, size=n)
    durations = rng.integers(5, 55, size=n)
    pickup_ts = [base + timedelta(minutes=int(m)) for m in pickup_offset]
    dropoff_ts = [t + timedelta(minutes=int(d)) for t, d in zip(pickup_ts, durations)]
    distance_km = np.round(rng.uniform(0.5, 25.0, size=n), 2)
    fare_usd = np.round(distance_km * rng.uniform(0.8, 1.6) + rng.uniform(1.0, 3.0), 2)
    cities = np.array(["AMS", "RTM", "EIN", "UTR", "HAG"])
    city = rng.choice(cities, size=n, replace=True)
    statuses = np.array(["completed", "cancelled", "no_show"])
    status = rng.choice(statuses, size=n, p=[0.85, 0.1, 0.05], replace=True)
    df = pd.DataFrame(
        {
            "ride_id": ride_id,
            "user_id": user_id,
            "driver_id": driver_id,
            "pickup_ts": pickup_ts,
            "dropoff_ts": dropoff_ts,
            "distance_km": distance_km,
            "fare_usd": fare_usd,
            "city": city,
            "status": status,
        }
    )
    return df


def generate(dataset: Literal["rides", "orders", "sensors"], n: int, day: str) -> pd.DataFrame:
    """Generate a synthetic DataFrame for a named dataset."""
    if dataset == "rides":
        return generate_rides(n, day)
    raise ValueError("Only 'rides' is implemented by default.")