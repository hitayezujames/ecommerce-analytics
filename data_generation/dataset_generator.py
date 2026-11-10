"""
dataset_generator.py  (improved version of the course-provided generator)
=========================================================================

Generates the synthetic e-commerce dataset (users, categories, products,
sessions, transactions) used throughout the project.

The page-flow model, probabilities and output format are kept IDENTICAL to the
provided script (see dataset_generator_original.py) so the data has the same
shape the brief describes. The changes are fixes and scalability improvements:

  1. products now carry a `subcategory_id` (the brief's product schema has one,
     the original script never assigned it).
  2. Reproducible: a fixed reference date (END_DATE) replaces datetime.now()/"now",
     and IDs are drawn from the seeded RNG instead of uuid4(), so the same seed
     always yields the same dataset.
  3. SCALE environment variable shrinks/grows sessions & transactions
     (e.g. SCALE=0.1 -> 200k sessions / 50k transactions) for laptops.
  4. Sessions are streamed to disk in 100k chunks instead of being held in
     memory (the original needs >8 GB RAM for 2M sessions).
  5. Inactive products can no longer be added to a cart (the original fallback
     path allowed it), and a cart whose quantities are all zero is no longer
     counted as "abandoned".
  6. Category lookups use a dict (O(1)) instead of a linear scan per page view.

Usage:
    python dataset_generator.py                    # full size (2M sessions)
    SCALE=0.1 OUTPUT_DIR=../data/raw python dataset_generator.py
"""
import datetime
import json
import os
import random
import threading

import numpy as np
from faker import Faker

# --- Configuration -----------------------------------------------------------
SCALE = float(os.environ.get("SCALE", "1.0"))
OUTPUT_DIR = os.environ.get("OUTPUT_DIR", ".")
os.makedirs(OUTPUT_DIR, exist_ok=True)

NUM_USERS = 10000
NUM_PRODUCTS = 5000
NUM_CATEGORIES = 25
NUM_TRANSACTIONS = int(500000 * SCALE)
NUM_SESSIONS = int(2000000 * SCALE)
TIMESPAN_DAYS = 90
MAX_ITERATIONS = (NUM_SESSIONS + NUM_TRANSACTIONS) * 2  # Fail-safe
CHUNK_SIZE = 100000

# Fixed "now" so every run is reproducible (override with END_DATE=YYYY-MM-DD)
END_DATE = datetime.datetime.fromisoformat(os.environ.get("END_DATE", "2026-10-01"))


def days_ago(n):
    return END_DATE - datetime.timedelta(days=n)


# --- Initialization ----------------------------------------------------------
np.random.seed(42)
random.seed(42)
Faker.seed(42)
fake = Faker()

print(f"Initializing dataset generation (SCALE={SCALE}, END_DATE={END_DATE.date()})...")


# --- ID Generators (seeded -> reproducible) -----------------------------------
def generate_session_id():
    return f"sess_{random.getrandbits(40):010x}"


def generate_transaction_id():
    return f"txn_{random.getrandbits(48):012x}"


# --- Inventory Management ----------------------------------------------------
class InventoryManager:
    def __init__(self, products):
        self.products = {p["product_id"]: p for p in products}
        self.lock = threading.RLock()  # For thread safety

    def update_stock(self, product_id, quantity):
        with self.lock:
            if product_id not in self.products:
                return False
            if self.products[product_id]["current_stock"] >= quantity:
                self.products[product_id]["current_stock"] -= quantity
                return True
            return False

    def get_product(self, product_id):
        with self.lock:
            return self.products.get(product_id)


# --- Helper Functions (page-flow model unchanged from the original) -----------
def determine_page_type(position, previous_pages):
    """Markov-style page transition model: next page depends on the previous one."""
    if position == 0:
        return random.choice(["home", "search", "category_listing"])
    if not previous_pages:
        return "home"

    prev_page = previous_pages[-1]["page_type"]
    transitions = {
        "home": (["category_listing", "search", "product_detail"], [0.5, 0.3, 0.2]),
        "category_listing": (["product_detail", "category_listing", "search", "home"], [0.7, 0.1, 0.1, 0.1]),
        "search": (["product_detail", "search", "category_listing", "home"], [0.6, 0.2, 0.1, 0.1]),
        "product_detail": (["product_detail", "cart", "category_listing", "search", "home"], [0.3, 0.3, 0.2, 0.1, 0.1]),
        "cart": (["checkout", "product_detail", "category_listing", "home"], [0.6, 0.2, 0.1, 0.1]),
        "checkout": (["confirmation", "cart", "home"], [0.8, 0.1, 0.1]),
        "confirmation": (["home", "product_detail", "category_listing"], [0.6, 0.2, 0.2]),
    }
    if prev_page not in transitions:
        return "home"
    pages, weights = transitions[prev_page]
    return random.choices(pages, weights=weights)[0]


def get_page_content(page_type, products_list, categories_by_id, categories_list):
    """Return (product, category) shown on a page. Prefers active, in-stock products."""
    if page_type == "product_detail":
        for _ in range(10):
            product = random.choice(products_list)
            if product["is_active"] and product["current_stock"] > 0:
                return product, categories_by_id[product["category_id"]]
        product = random.choice(products_list)  # fallback: any product
        return product, categories_by_id[product["category_id"]]
    if page_type == "category_listing":
        return None, random.choice(categories_list)
    return None, None


def build_transaction(user_id, session_id, timestamp, items, payment_methods, statuses):
    subtotal = round(sum(item["subtotal"] for item in items), 2)
    discount = 0
    if random.random() < 0.2:  # 20% chance of discount
        discount = round(subtotal * random.choice([0.05, 0.1, 0.15, 0.2]), 2)
    return {
        "transaction_id": generate_transaction_id(),
        "session_id": session_id,
        "user_id": user_id,
        "timestamp": timestamp,
        "items": items,
        "subtotal": subtotal,
        "discount": discount,
        "total": round(subtotal - discount, 2),
        "payment_method": random.choice(payment_methods),
        "status": random.choice(statuses),
    }


# --- Category Generation -----------------------------------------------------
categories = []
for cat_id in range(NUM_CATEGORIES):
    category = {"category_id": f"cat_{cat_id:03d}", "name": fake.company(), "subcategories": []}
    for sub_id in range(random.randint(3, 5)):
        category["subcategories"].append({
            "subcategory_id": f"sub_{cat_id:03d}_{sub_id:02d}",
            "name": fake.bs(),
            "profit_margin": round(random.uniform(0.1, 0.4), 2),
        })
    categories.append(category)
categories_by_id = {c["category_id"]: c for c in categories}
print(f"Generated {len(categories)} categories")

# --- Product Generation ------------------------------------------------------
products = []
product_creation_start = days_ago(TIMESPAN_DAYS * 2)

for prod_id in range(NUM_PRODUCTS):
    category = random.choice(categories)
    subcategory = random.choice(category["subcategories"])  # FIX 1

    base_price = round(random.uniform(5, 500), 2)
    initial_date = fake.date_time_between(
        start_date=product_creation_start,
        end_date=product_creation_start + datetime.timedelta(days=TIMESPAN_DAYS // 3),
    )
    price_history = [{"price": base_price, "date": initial_date.isoformat()}]
    for _ in range(random.randint(0, 2)):  # 0-2 further price changes (+/-20%)
        price_change_date = fake.date_time_between(start_date=initial_date, end_date=END_DATE)
        price_history.append({
            "price": round(base_price * random.uniform(0.8, 1.2), 2),
            "date": price_change_date.isoformat(),
        })
        initial_date = price_change_date
    price_history.sort(key=lambda x: x["date"])

    products.append({
        "product_id": f"prod_{prod_id:05d}",
        "name": fake.catch_phrase().title(),
        "category_id": category["category_id"],
        "subcategory_id": subcategory["subcategory_id"],
        "base_price": price_history[-1]["price"],  # current price
        "current_stock": random.randint(10, 1000),
        "is_active": random.choices([True, False], weights=[0.95, 0.05])[0],
        "price_history": price_history,
        "creation_date": price_history[0]["date"],
    })
print(f"Generated {len(products)} products")

# --- User Generation ---------------------------------------------------------
users = []
for user_id in range(NUM_USERS):
    reg_date = fake.date_time_between(start_date=days_ago(TIMESPAN_DAYS * 3), end_date=days_ago(TIMESPAN_DAYS))
    users.append({
        "user_id": f"user_{user_id:06d}",
        "geo_data": {"city": fake.city(), "state": fake.state_abbr(), "country": fake.country_code()},
        "registration_date": reg_date.isoformat(),
        "last_active": fake.date_time_between(start_date=reg_date, end_date=END_DATE).isoformat(),
    })
print(f"Generated {len(users)} users")

# --- Session & Transaction Generation ----------------------------------------
inventory = InventoryManager(products)
session_buffer = []
session_chunk_no = 0
transactions = []
transaction_counter = 0
session_counter = 0
iteration = 0


def flush_sessions(force=False):
    """FIX 4: stream sessions to disk in chunks instead of keeping 2M in RAM."""
    global session_buffer, session_chunk_no
    if session_buffer and (force or len(session_buffer) >= CHUNK_SIZE):
        with open(os.path.join(OUTPUT_DIR, f"sessions_{session_chunk_no}.json"), "w") as f:
            json.dump(session_buffer, f)
        session_chunk_no += 1
        session_buffer = []


print("Generating sessions and transactions...")
while (session_counter < NUM_SESSIONS or transaction_counter < NUM_TRANSACTIONS) and iteration < MAX_ITERATIONS:
    iteration += 1

    # ---- Session generation ----
    if session_counter < NUM_SESSIONS:
        user = random.choice(users)
        session_id = generate_session_id()
        session_start = fake.date_time_between(start_date=days_ago(TIMESPAN_DAYS), end_date=END_DATE)
        session_duration = random.randint(30, 3600)

        page_views, viewed_products, cart_contents = [], set(), {}
        time_slots = sorted([0] + [random.randint(1, session_duration - 1)
                                   for _ in range(random.randint(3, 15))] + [session_duration])

        for i in range(len(time_slots) - 1):
            page_type = determine_page_type(i, page_views)
            product, category = get_page_content(page_type, products, categories_by_id, categories)

            if page_type == "product_detail" and product:
                product_id = product["product_id"]
                viewed_products.add(product_id)
                # FIX 5: only active products can be added to the cart
                if product["is_active"] and random.random() < 0.3:
                    entry = cart_contents.setdefault(product_id, {"quantity": 0, "price": product["base_price"]})
                    max_possible = min(3, inventory.get_product(product_id)["current_stock"] - entry["quantity"])
                    if max_possible > 0:
                        entry["quantity"] += random.randint(1, max_possible)

            page_views.append({
                "timestamp": (session_start + datetime.timedelta(seconds=time_slots[i])).isoformat(),
                "page_type": page_type,
                "product_id": product["product_id"] if product else None,
                "category_id": category["category_id"] if category else None,
                "view_duration": time_slots[i + 1] - time_slots[i],
            })

        cart_contents = {k: v for k, v in cart_contents.items() if v["quantity"] > 0}
        converted = False
        if cart_contents and any(p["page_type"] in ("checkout", "confirmation") for p in page_views):
            converted = random.random() < 0.7

        session_geo = dict(user["geo_data"], ip_address=fake.ipv4())
        session_end = session_start + datetime.timedelta(seconds=session_duration)
        session_buffer.append({
            "session_id": session_id,
            "user_id": user["user_id"],
            "start_time": session_start.isoformat(),
            "end_time": session_end.isoformat(),
            "duration_seconds": session_duration,
            "geo_data": session_geo,
            "device_profile": {
                "type": random.choice(["mobile", "desktop", "tablet"]),
                "os": random.choice(["iOS", "Android", "Windows", "macOS"]),
                "browser": random.choice(["Chrome", "Safari", "Firefox", "Edge"]),
            },
            "viewed_products": sorted(viewed_products),
            "page_views": page_views,
            "cart_contents": cart_contents,
            "conversion_status": "converted" if converted else "abandoned" if cart_contents else "browsed",
            "referrer": random.choice(["direct", "email", "social", "search_engine", "affiliate"]),
        })
        session_counter += 1

        # Transaction for a converted session (all-or-nothing on stock)
        if converted and transaction_counter < NUM_TRANSACTIONS:
            items, valid = [], True
            for prod_id, details in cart_contents.items():
                if inventory.update_stock(prod_id, details["quantity"]):
                    items.append({
                        "product_id": prod_id,
                        "quantity": details["quantity"],
                        "unit_price": details["price"],
                        "subtotal": round(details["quantity"] * details["price"], 2),
                    })
                else:
                    valid = False
                    break
            if valid and items:
                transactions.append(build_transaction(
                    user["user_id"], session_id, session_end.isoformat(), items,
                    ["credit_card", "paypal", "apple_pay", "crypto"], ["completed"]))
                transaction_counter += 1

        flush_sessions()

    # ---- Additional (non-session, e.g. app/phone) transactions ----
    if transaction_counter < NUM_TRANSACTIONS and random.random() < 0.2:
        user = random.choice(users)
        items = []
        for product in random.sample(products, k=min(3, len(products))):
            if product["is_active"]:
                quantity = random.randint(1, 3)
                if inventory.update_stock(product["product_id"], quantity):
                    items.append({
                        "product_id": product["product_id"],
                        "quantity": quantity,
                        "unit_price": product["base_price"],
                        "subtotal": round(quantity * product["base_price"], 2),
                    })
        if items:
            ts = fake.date_time_between(start_date=days_ago(TIMESPAN_DAYS), end_date=END_DATE).isoformat()
            transactions.append(build_transaction(
                user["user_id"], None, ts, items,
                ["credit_card", "paypal", "bank_transfer", "gift_card"],
                ["completed", "processing", "shipped", "delivered"]))
            transaction_counter += 1

    if iteration % 50000 == 0:
        print(f"Progress: {session_counter:,}/{NUM_SESSIONS:,} sessions, "
              f"{transaction_counter:,}/{NUM_TRANSACTIONS:,} transactions (iteration {iteration:,})")

flush_sessions(force=True)

# --- Data Export -------------------------------------------------------------
print("Saving datasets...")
for name, payload in [("users", users),
                      ("products", list(inventory.products.values())),  # stock after sales
                      ("categories", categories),
                      ("transactions", transactions)]:
    with open(os.path.join(OUTPUT_DIR, f"{name}.json"), "w") as f:
        json.dump(payload, f)

print(f"""
Dataset generation complete!
- Sessions: {session_counter:,} (target: {NUM_SESSIONS:,}) in {session_chunk_no} file(s)
- Transactions: {len(transactions):,} (target: {NUM_TRANSACTIONS:,})
- Remaining stock units: {sum(p['current_stock'] for p in inventory.products.values()):,}
""")
