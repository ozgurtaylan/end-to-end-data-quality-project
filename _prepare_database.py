from sqlalchemy import create_engine, text
import random
import logging

logging.basicConfig(level=logging.INFO)

# 1M
DATA_COUNT = 1000000
engine = create_engine('mysql+mysqlconnector://root:root@localhost:3377/inventory')
valid_categories = ["Elektronik", "Moda", "Ev & Yaşam", "Spor", "Otomotiv"]
invalid_categories = ["Gıda", "Bilinmeyen", "Eğlence", None]

def create_and_clear_table():
    try:
        with engine.begin() as conn:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS products (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    name VARCHAR(255),
                    category VARCHAR(100),
                    price FLOAT,
                    stock INT
                )
            """))
            conn.execute(text("TRUNCATE TABLE products"))
        logging.info("Table created and cleared successfully.")
    except Exception as e:
        logging.error(f"Error while creating or clearing the table: {e}")
        raise

def generate_products():
    return [
        (   
            f'Product {i}',
            random.choice(valid_categories + invalid_categories),
            random.choices([round(random.uniform(-50.0, 500.0), 2), None], weights=[95, 5])[0],
            random.choices([random.randint(-50, 5000), None], weights=[95, 5])[0]
        )
        for i in range(1, DATA_COUNT + 1)
    ]

def insert_products(products):
    try:
        insert_query = text("INSERT INTO products (name, category, price, stock) VALUES (:name, :category, :price, :stock)")
        with engine.begin() as conn:
            conn.execute(insert_query, [
                {"name": name, "category": category, "price": price, "stock": stock}
                for name, category, price, stock in products
            ])
        logging.info(f"Inserted {DATA_COUNT} products successfully.")
    except Exception as e:
        logging.error(f"Error while inserting data: {e}")
        raise

def main():
    create_and_clear_table()
    products = generate_products()
    insert_products(products)

if __name__ == "__main__":
    main()
