from fastapi import FastAPI, HTTPException, Depends
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from sqlalchemy import text
from google.cloud.sql.connector import Connector
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
    # Create a secure connection to Cloud SQL using asyncpg for async support
    connection = connector.connect(
        INSTANCE_CONNECTION_NAME,
        "pymysql",  # Async MySQL equivalent should be configured
        user=DB_USER,
        password=DB_PASS,
        db=DB_NAME
    )
    return create_async_engine("mysql+aiomysql://", creator=lambda: connection)


# Create a database session dependency
async_session = sessionmaker(
    bind=get_engine(),
    class_=AsyncSession,
    expire_on_commit=False,
)


async def get_db():
    async with async_session() as session:
        yield session


# Function to fetch the main product by UPC
async def fetch_product_by_upc(db: AsyncSession, upc: str):
    query = text("SELECT * FROM products WHERE upc = :upc")
    result = await db.execute(query, {"upc": upc})
    return result.fetchone()


# Function to fetch the product ID by alternate UPC
async def fetch_product_id_by_alternate_upc(db: AsyncSession, upc: str):
    query = text("SELECT product_id FROM product_alternates WHERE upc = :upc")
    result = await db.execute(query, {"upc": upc})
    row = result.fetchone()
    return row.product_id if row else None


# Function to fetch alternates (variants and cases) for a given product ID
async def fetch_alternates_by_product_id(db: AsyncSession, product_id: int):
    query = text("SELECT upc, alternate_type, case_pack FROM product_alternates WHERE product_id = :product_id")
    result = await db.execute(query, {"product_id": product_id})
    return result.fetchall()


# Endpoint to get product details
@app.get("/v1/product/{upc}")
async def get_product_details(upc: str, db: AsyncSession = Depends(get_db)):
    # Validate UPC length
    if len(upc) != 14:
        raise HTTPException(status_code=400, detail="UPC must be 14 digits long")

    # Step 1: Try to find the product in the main products table
    product = await fetch_product_by_upc(db, upc)

    # Step 2: If not found, check if it's an alternate UPC in product_alternates
    if not product:
        product_id = await fetch_product_id_by_alternate_upc(db, upc)
        if not product_id:
            raise HTTPException(status_code=404, detail="Product not found")

        # Fetch the main product using the product_id from the alternate table
        query = text("SELECT * FROM products WHERE product_id = :product_id")
        result = await db.execute(query, {"product_id": product_id})
        product = result.fetchone()

    # If still not found, return 404
    if not product:
        raise HTTPException(status_code=404, detail="Product not found")

    # Step 3: Fetch any alternates (variants or cases) for this product ID
    alternates = await fetch_alternates_by_product_id(db, product.product_id)

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