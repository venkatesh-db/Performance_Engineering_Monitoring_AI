"""Generate weighted CSV test data. Skew mirrors production: a few customers dominate."""
import random, csv
random.seed(42)
with open("customers.csv", "w", newline="") as f:
    w = csv.writer(f)
    for _ in range(10):
        for i in range(1, 21):
            w.writerow([f"CUST{i:04d}", random.choice([500, 1500, 2300, 890, 4500])])
    for i in range(21, 501):
        w.writerow([f"CUST{i:04d}", random.choice([500, 1500, 2300, 890, 4500])])
print("customers.csv written")
