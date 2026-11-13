CSV_COLUMNS = [
    "trans_date_trans_time",
    "cc_num",
    "merchant",
    "category",
    "amt",
    "first",
    "last",
    "gender",
    "street",
    "city",
    "state",
    "zip",
    "lat",
    "long",
    "city_pop",
    "job",
    "dob",
    "trans_num",
    "unix_time",
    "merch_lat",
    "merch_long",
    "is_fraud",
]

TOTAL_ROWS = 1_852_394

# First trans_date_trans_time of fraudTest.csv: rows from here on are the test split.
TEST_SPLIT_TS = "2020-06-21 12:14:25"

CATEGORIES = [
    "entertainment",
    "food_dining",
    "gas_transport",
    "grocery_net",
    "grocery_pos",
    "health_fitness",
    "home",
    "kids_pets",
    "misc_net",
    "misc_pos",
    "personal_care",
    "shopping_net",
    "shopping_pos",
    "travel",
]

BRONZE_TABLE = "bronze.transactions_raw"
SILVER_TABLE = "silver.transactions"
REJECTED_TABLE = "silver.transactions_rejected"

SILVER_COLUMNS = [
    "trans_num",
    "trans_ts",
    "event_ts",
    "card_hash",
    "merchant",
    "category",
    "amt",
    "gender",
    "city",
    "state",
    "zip",
    "lat",
    "long",
    "merch_lat",
    "merch_long",
    "city_pop",
    "job",
    "age",
    "distance_km",
    "hour_of_day",
    "day_of_week",
    "is_fraud",
    "ingest_ts",
]
