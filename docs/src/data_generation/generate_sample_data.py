"""
Generate synthetic e-commerce CSV datasets for the medallion pipeline assessment.

Produces customers.csv, orders.csv, and products.csv with exact row counts and
intentional data-quality defects required by the assessment specification.
"""

from __future__ import annotations

import argparse
import random
import sys
from datetime import date, timedelta
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path

import pandas as pd
from faker import Faker

# ---------------------------------------------------------------------------
# Reproducibility
# ---------------------------------------------------------------------------
RANDOM_SEED = 42

# ---------------------------------------------------------------------------
# Target volumes (assessment specification)
# ---------------------------------------------------------------------------
CUSTOMER_ROW_COUNT = 10_000
PRODUCT_ROW_COUNT = 500
ORDER_ROW_COUNT = 100_000

UNIQUE_CUSTOMER_COUNT = 9_990  # 10_000 total rows includes 10 duplicate-id rows
UNIQUE_ORDER_COUNT = 99_980  # 100_000 total rows includes 20 duplicate-id rows

# ---------------------------------------------------------------------------
# Intentional quality issue counts (assessment specification — do not change)
# ---------------------------------------------------------------------------
NULL_EMAIL_COUNT = 50
DUPLICATE_CUSTOMER_ID_COUNT = 10

NULL_CUSTOMER_ID_COUNT = 100
NULL_PRODUCT_ID_COUNT = 200
INVALID_CUSTOMER_ID_COUNT = 50
INVALID_PRODUCT_ID_COUNT = 30
DUPLICATE_ORDER_ID_COUNT = 20

# Valid FK ranges derived from clean master data
VALID_CUSTOMER_ID_MIN = 1
VALID_CUSTOMER_ID_MAX = UNIQUE_CUSTOMER_COUNT  # 9_990
INVALID_CUSTOMER_ID_START = 99_991  # not present in customers

VALID_PRODUCT_ID_MIN = 1
VALID_PRODUCT_ID_MAX = PRODUCT_ROW_COUNT  # 500
INVALID_PRODUCT_ID_START = 501  # not present in products

CUSTOMER_SEGMENTS = ("Premium", "Standard", "Basic")
ORDER_STATUSES = ("Pending", "Completed", "Cancelled")
PRODUCT_CATEGORIES = (
    "Electronics",
    "Clothing",
    "Home & Garden",
    "Sports",
    "Books",
    "Beauty",
    "Toys",
    "Automotive",
    "Health",
    "Office",
)

DEFAULT_OUTPUT_DIR = Path(__file__).resolve().parents[2] / "data" / "raw"


class GenerationValidationError(Exception):
    """Raised when generated data does not meet expected counts or constraints."""


def _money(value: float) -> float:
    return float(Decimal(str(value)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))


def _init_random() -> Faker:
    random.seed(RANDOM_SEED)
    faker = Faker()
    faker.seed_instance(RANDOM_SEED)
    return faker


def _random_date_between(start: date, end: date) -> date:
    delta_days = (end - start).days
    return start + timedelta(days=random.randint(0, delta_days))


def generate_products(faker: Faker) -> pd.DataFrame:
    """Generate 500 clean product records (no intentional defects)."""
    rows: list[dict] = []
    for product_id in range(1, PRODUCT_ROW_COUNT + 1):
        category = random.choice(PRODUCT_CATEGORIES)
        price = _money(random.uniform(5.0, 500.0))
        cost = _money(price * random.uniform(0.35, 0.75))
        stock_quantity = random.randint(0, 2_000)
        reorder_level = random.randint(10, 200)
        rows.append(
            {
                "product_id": product_id,
                "product_name": f"{faker.word().capitalize()} {category.split()[0]} {product_id:04d}",
                "category": category,
                "price": price,
                "cost": cost,
                "stock_quantity": stock_quantity,
                "reorder_level": reorder_level,
            }
        )
    return pd.DataFrame(rows)


def generate_customers(faker: Faker) -> pd.DataFrame:
  """
  Generate 10,000 customer rows:
  - 9,990 unique customer_id values (1..9990)
  - 10 additional rows that duplicate customer_id 1..10
  - 50 rows with NULL/empty email (on unique-id rows only)
  """
  signup_start = date(2015, 1, 1)
  signup_end = date(2025, 12, 31)
  segment_weights = [0.15, 0.55, 0.30]

  rows: list[dict] = []
  for customer_id in range(1, UNIQUE_CUSTOMER_COUNT + 1):
    segment = random.choices(CUSTOMER_SEGMENTS, weights=segment_weights, k=1)[0]
    lifetime_value = _money(random.uniform(250.0, 75_000.0))
    rows.append(
      {
        "customer_id": customer_id,
        "customer_name": faker.name(),
        "email": faker.email(),
        "country": faker.country(),
        "signup_date": _random_date_between(signup_start, signup_end).isoformat(),
        "customer_segment": segment,
        "lifetime_value": lifetime_value,
      }
    )

  df = pd.DataFrame(rows)

  # INTENTIONAL BAD DATA: 50 NULL email values on unique customer rows (ids 1001..1050).
  null_email_ids = list(range(1001, 1001 + NULL_EMAIL_COUNT))
  df.loc[df["customer_id"].isin(null_email_ids), "email"] = ""

  duplicate_source_ids = list(range(1, DUPLICATE_CUSTOMER_ID_COUNT + 1))
  duplicate_rows = df.loc[df["customer_id"].isin(duplicate_source_ids)].copy()
  # Slightly alter duplicate rows so they are visibly separate records sharing the same PK.
  duplicate_rows["customer_name"] = duplicate_rows["customer_name"] + " (dup)"
  duplicate_rows["email"] = duplicate_rows["email"].apply(
    lambda e: f"dup_{e}" if e else ""
  )

  # INTENTIONAL BAD DATA: 10 duplicate customer_id records (second copies of ids 1..10).
  df = pd.concat([df, duplicate_rows], ignore_index=True)

  return df


def _build_valid_order_row(
    order_id: int,
    faker: Faker,
    order_start: date,
    order_end: date,
) -> dict:
    customer_id = random.randint(VALID_CUSTOMER_ID_MIN, VALID_CUSTOMER_ID_MAX)
    product_id = random.randint(VALID_PRODUCT_ID_MIN, VALID_PRODUCT_ID_MAX)
    order_date = _random_date_between(order_start, order_end)
    quantity = random.randint(1, 10)
    unit_price = _money(random.uniform(5.0, 450.0))
    total_amount = _money(quantity * unit_price)
    order_status = random.choices(
        ORDER_STATUSES,
        weights=[0.10, 0.82, 0.08],
        k=1,
    )[0]

    if order_status == "Completed":
        max_payment_offset = (order_end - order_date).days
        offset = random.randint(0, max_payment_offset) if max_payment_offset > 0 else 0
        payment_date = (order_date + timedelta(days=offset)).isoformat()
    else:
        payment_date = ""

    return {
        "order_id": order_id,
        "customer_id": customer_id,
        "order_date": order_date.isoformat(),
        "product_id": product_id,
        "quantity": quantity,
        "unit_price": unit_price,
        "total_amount": total_amount,
        "order_status": order_status,
        "payment_date": payment_date,
    }


def generate_orders(faker: Faker) -> pd.DataFrame:
  """
  Generate 100,000 order rows:
  - 99,980 primary rows with order_id 1..99980
  - 20 additional rows duplicating order_id 1..20
  - Disjoint intentional defects on primary rows only
  """
  order_start = date(2020, 1, 1)
  order_end = date(2025, 12, 31)

  rows = [
    _build_valid_order_row(order_id, faker, order_start, order_end)
    for order_id in range(1, UNIQUE_ORDER_COUNT + 1)
  ]
  df = pd.DataFrame(rows)

  primary_indices = [
    i for i in range(len(df)) if int(df.at[i, "order_id"]) > DUPLICATE_ORDER_ID_COUNT
  ]
  random.shuffle(primary_indices)

  required_defect_rows = (
    NULL_CUSTOMER_ID_COUNT
    + NULL_PRODUCT_ID_COUNT
    + INVALID_CUSTOMER_ID_COUNT
    + INVALID_PRODUCT_ID_COUNT
  )
  if len(primary_indices) < required_defect_rows:
    raise GenerationValidationError(
      f"Not enough primary order rows for defects: need {required_defect_rows}, "
      f"have {len(primary_indices)}"
    )

  null_customer_idx = primary_indices[:NULL_CUSTOMER_ID_COUNT]
  null_product_idx = primary_indices[NULL_CUSTOMER_ID_COUNT:NULL_CUSTOMER_ID_COUNT + NULL_PRODUCT_ID_COUNT]
  invalid_customer_idx = primary_indices[
    NULL_CUSTOMER_ID_COUNT + NULL_PRODUCT_ID_COUNT:NULL_CUSTOMER_ID_COUNT + NULL_PRODUCT_ID_COUNT + INVALID_CUSTOMER_ID_COUNT
  ]
  invalid_product_idx = primary_indices[
    NULL_CUSTOMER_ID_COUNT + NULL_PRODUCT_ID_COUNT + INVALID_CUSTOMER_ID_COUNT:NULL_CUSTOMER_ID_COUNT + NULL_PRODUCT_ID_COUNT + INVALID_CUSTOMER_ID_COUNT + INVALID_PRODUCT_ID_COUNT
  ]

  # INTENTIONAL BAD DATA: 100 NULL customer_id values.
  df.loc[null_customer_idx, "customer_id"] = pd.NA

  # INTENTIONAL BAD DATA: 200 NULL product_id values.
  df.loc[null_product_idx, "product_id"] = pd.NA

  # INTENTIONAL BAD DATA: 50 customer_id values not present in customers (99_991..100_040).
  for i, row_idx in enumerate(invalid_customer_idx):
    df.at[row_idx, "customer_id"] = INVALID_CUSTOMER_ID_START + i

  # INTENTIONAL BAD DATA: 30 product_id values not present in products (501..530).
  for i, row_idx in enumerate(invalid_product_idx):
    df.at[row_idx, "product_id"] = INVALID_PRODUCT_ID_START + i

  duplicate_source_ids = list(range(1, DUPLICATE_ORDER_ID_COUNT + 1))
  duplicate_rows = df.loc[df["order_id"].isin(duplicate_source_ids)].copy()
  duplicate_rows["customer_name_marker"] = "dup"  # temporary marker column

  # INTENTIONAL BAD DATA: 20 duplicate order_id rows (second copies of order_id 1..20).
  duplicate_rows = duplicate_rows.drop(columns=["customer_name_marker"])
  df = pd.concat([df, duplicate_rows], ignore_index=True)

  return df


def _count_duplicate_key_rows(df: pd.DataFrame, key: str) -> int:
  """Count rows that duplicate an earlier row for the given key (second+ occurrences)."""
  return int(df.duplicated(subset=[key], keep="first").sum())


def _count_null_or_empty(series: pd.Series) -> int:
  as_str = series.astype("string")
  return int(as_str.isna().sum() + (as_str.str.strip() == "").sum())


def _count_invalid_customer_fk(df: pd.DataFrame, valid_ids: set[int]) -> int:
  non_null = df["customer_id"].dropna()
  return int((~non_null.isin(valid_ids)).sum())


def _count_invalid_product_fk(df: pd.DataFrame, valid_ids: set[int]) -> int:
  non_null = df["product_id"].dropna()
  return int((~non_null.isin(valid_ids)).sum())


def validate_customers(df: pd.DataFrame) -> None:
  errors: list[str] = []

  if len(df) != CUSTOMER_ROW_COUNT:
    errors.append(f"customers row count: expected {CUSTOMER_ROW_COUNT}, got {len(df)}")

  null_email_count = _count_null_or_empty(df["email"])
  if null_email_count != NULL_EMAIL_COUNT:
    errors.append(
      f"NULL/empty email count: expected {NULL_EMAIL_COUNT}, got {null_email_count}"
    )

  duplicate_count = _count_duplicate_key_rows(df, "customer_id")
  if duplicate_count != DUPLICATE_CUSTOMER_ID_COUNT:
    errors.append(
      f"duplicate customer_id rows: expected {DUPLICATE_CUSTOMER_ID_COUNT}, got {duplicate_count}"
    )

  dup_ids = df[df.duplicated(subset=["customer_id"], keep=False)]["customer_id"].unique()
  if len(dup_ids) != DUPLICATE_CUSTOMER_ID_COUNT:
    errors.append(
      f"customer_ids involved in duplication: expected {DUPLICATE_CUSTOMER_ID_COUNT}, got {len(dup_ids)}"
    )

  id_counts = df.groupby("customer_id").size()
  if (id_counts > 2).any():
    errors.append("unexpected customer_id with more than two rows")

  if df["customer_id"].isna().any():
    errors.append("unexpected NULL customer_id values in customers")

  if errors:
    raise GenerationValidationError("Customer validation failed:\n  - " + "\n  - ".join(errors))


def validate_products(df: pd.DataFrame) -> None:
  errors: list[str] = []

  if len(df) != PRODUCT_ROW_COUNT:
    errors.append(f"products row count: expected {PRODUCT_ROW_COUNT}, got {len(df)}")

  if _count_duplicate_key_rows(df, "product_id") != 0:
    errors.append("unexpected duplicate product_id rows in products")

  if df["product_id"].isna().any():
    errors.append("unexpected NULL product_id values in products")

  if errors:
    raise GenerationValidationError("Product validation failed:\n  - " + "\n  - ".join(errors))


def validate_orders(df: pd.DataFrame, valid_customer_ids: set[int], valid_product_ids: set[int]) -> None:
  errors: list[str] = []

  if len(df) != ORDER_ROW_COUNT:
    errors.append(f"orders row count: expected {ORDER_ROW_COUNT}, got {len(df)}")

  null_customer_count = int(df["customer_id"].isna().sum())
  if null_customer_count != NULL_CUSTOMER_ID_COUNT:
    errors.append(
      f"NULL customer_id count: expected {NULL_CUSTOMER_ID_COUNT}, got {null_customer_count}"
    )

  null_product_count = int(df["product_id"].isna().sum())
  if null_product_count != NULL_PRODUCT_ID_COUNT:
    errors.append(
      f"NULL product_id count: expected {NULL_PRODUCT_ID_COUNT}, got {null_product_count}"
    )

  invalid_customer_count = _count_invalid_customer_fk(df, valid_customer_ids)
  if invalid_customer_count != INVALID_CUSTOMER_ID_COUNT:
    errors.append(
      f"invalid customer_id count: expected {INVALID_CUSTOMER_ID_COUNT}, got {invalid_customer_count}"
    )

  invalid_product_count = _count_invalid_product_fk(df, valid_product_ids)
  if invalid_product_count != INVALID_PRODUCT_ID_COUNT:
    errors.append(
      f"invalid product_id count: expected {INVALID_PRODUCT_ID_COUNT}, got {invalid_product_count}"
    )

  duplicate_count = _count_duplicate_key_rows(df, "order_id")
  if duplicate_count != DUPLICATE_ORDER_ID_COUNT:
    errors.append(
      f"duplicate order_id rows: expected {DUPLICATE_ORDER_ID_COUNT}, got {duplicate_count}"
    )

  dup_ids = df[df.duplicated(subset=["order_id"], keep=False)]["order_id"].unique()
  if len(dup_ids) != DUPLICATE_ORDER_ID_COUNT:
    errors.append(
      f"order_ids involved in duplication: expected {DUPLICATE_ORDER_ID_COUNT}, got {len(dup_ids)}"
    )

  # Clean-row sanity: rows outside defect sets should have valid FKs and amounts consistent.
  clean_mask = (
    df["customer_id"].notna()
    & df["product_id"].notna()
    & df["customer_id"].isin(valid_customer_ids)
    & df["product_id"].isin(valid_product_ids)
    & ~df.duplicated(subset=["order_id"], keep="first")
  )
  clean = df.loc[clean_mask]
  amount_mismatch = (
    clean["total_amount"].round(2)
    != (clean["quantity"] * clean["unit_price"]).round(2)
  ).sum()
  if amount_mismatch:
    errors.append(f"unexpected total_amount mismatch on {amount_mismatch} clean order rows")

  if errors:
    raise GenerationValidationError("Order validation failed:\n  - " + "\n  - ".join(errors))


def write_csv(df: pd.DataFrame, path: Path) -> None:
  path.parent.mkdir(parents=True, exist_ok=True)
  df.to_csv(path, index=False, na_rep="")


def generate_all(output_dir: Path) -> dict[str, Path]:
  faker = _init_random()

  products = generate_products(faker)
  customers = generate_customers(faker)
  orders = generate_orders(faker)

  valid_customer_ids = set(range(VALID_CUSTOMER_ID_MIN, VALID_CUSTOMER_ID_MAX + 1))
  valid_product_ids = set(range(VALID_PRODUCT_ID_MIN, VALID_PRODUCT_ID_MAX + 1))

  validate_products(products)
  validate_customers(customers)
  validate_orders(orders, valid_customer_ids, valid_product_ids)

  paths = {
    "customers": output_dir / "customers.csv",
    "orders": output_dir / "orders.csv",
    "products": output_dir / "products.csv",
  }

  write_csv(customers, paths["customers"])
  write_csv(orders, paths["orders"])
  write_csv(products, paths["products"])

  return paths


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
  parser = argparse.ArgumentParser(description="Generate assessment e-commerce CSV datasets.")
  parser.add_argument(
    "--output-dir",
    type=Path,
    default=DEFAULT_OUTPUT_DIR,
    help=f"Directory for CSV output (default: {DEFAULT_OUTPUT_DIR})",
  )
  return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
  args = parse_args(argv)
  try:
    paths = generate_all(args.output_dir)
  except GenerationValidationError as exc:
    print(f"ERROR: {exc}", file=sys.stderr)
    return 1

  print("Sample data generated successfully:")
  for name, path in paths.items():
    print(f"  {name}: {path}")
  return 0


if __name__ == "__main__":
  raise SystemExit(main())
