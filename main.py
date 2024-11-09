from fastapi import FastAPI, HTTPException, Depends
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, Session
from google.cloud.sql.connector import Connector
import sqlalchemy
import os
import uvicorn
import yaml

# Initialize the FastAPI app
app = FastAPI()

# Set up database connection details
with open('./secrets/config.yaml') as f:
    config = yaml.safe_load(f)
    DB_USER = config['DB_USER']
    DB_PASS = config['DB_PASS']
    DB_NAME = config['DB_NAME']
    INSTANCE_CONNECTION_NAME = config['INSTANCE_CONNECTION_NAME']

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = './secrets/gcp_key.json'
# Initialize Cloud SQL Connector and SQLAlchemy engine
connector = Connector()


def get_engine():
    # Create a secure connection to Cloud SQL
    connection = connector.connect(
        INSTANCE_CONNECTION_NAME,
        "pymysql",
        user=DB_USER,
        password=DB_PASS,
        db=DB_NAME
    )
    return sqlalchemy.create_engine("mysql+pymysql://", creator=lambda: connection)


# Create a database session dependency
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=get_engine())


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


# Function to fetch the main product by UPC
def fetch_product_by_upc(db: Session, upc: str):
    query = text("SELECT * FROM products WHERE upc = :upc")
    result = db.execute(query, {"upc": upc}).fetchone()
    return result


# Function to fetch the product ID by alternate UPC
def fetch_product_id_by_alternate_upc(db: Session, upc: str):
    query = text("SELECT product_id FROM product_alternates WHERE upc = :upc")
    result = db.execute(query, {"upc": upc}).fetchone()
    return result.product_id if result else None


# Function to fetch alternates (variants and cases) for a given product ID
def fetch_alternates_by_product_id(db: Session, product_id: int):
    query = text("SELECT upc, alternate_type, case_pack FROM product_alternates WHERE product_id = :product_id")
    return db.execute(query, {"product_id": product_id}).fetchall()


# Endpoint to get product details
@app.get("/v1/product/{upc}")
def get_product_details(upc: str, db: Session = Depends(get_db)):
    # Validate UPC length
    if len(upc) != 14:
        raise HTTPException(status_code=400, detail="UPC must be 14 digits long")

    # Step 1: Try to find the product in the main products table
    product = fetch_product_by_upc(db, upc)

    # Step 2: If not found, check if it's an alternate UPC in product_alternates
    if not product:
        product_id = fetch_product_id_by_alternate_upc(db, upc)
        if not product_id:
            raise HTTPException(status_code=404, detail="Product not found")

        # Fetch the main product using the product_id from the alternate table
        product = db.execute(
            text("SELECT * FROM products WHERE product_id = :product_id"),
            {"product_id": product_id}
        ).fetchone()

    # If still not found, return 404
    if not product:
        raise HTTPException(status_code=404, detail="Product not found")

    # Step 3: Fetch any alternates (variants or cases) for this product ID
    alternates = fetch_alternates_by_product_id(db, product.product_id)

    # Format the alternates for the response
    variants = [
        {
            "upc": alt.upc,
            "type": alt.alternate_type,
            "case_pack": alt.case_pack
        }
        for alt in alternates
    ]

    # Build and return the response
    return {
        "name": product.name,
        "item_number": product.item_number,
        "price": product.price,
        "supplier": product.supplier,
        "inventory_level": product.inventory_level,
        "inventory_updated_at": product.inventory_updated_at,
        "variants": variants
    }


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8080)