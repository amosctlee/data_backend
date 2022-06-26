from typing import Optional
from fastapi import FastAPI, status
from pydantic import BaseModel
from databases import Database

import sqlalchemy as sa

import db_model

from settings import settings

engine = sa.create_engine(settings.DB_URI)
db_model.Base.metadata.create_all(engine)

database = Database(settings.DB_URI)

app = FastAPI(
    title="Price comparing API",
    description="This is a price comparing API of e-commerce platforms",
    version="0.0.1"
)


@app.on_event("startup")
async def startup():
    await database.connect()


@app.on_event("shutdown")
async def shutdown():
    await database.disconnect()


@app.get("/product/")
async def search_product(
    product_name: str, shop_name: str = "google.tw"
) -> dict:
    product_models_stmt = """
    WITH shop_product_history AS (
        SELECT
            shop.username, shop.shopid, shop.brand_name,
            product.itemid, product.name AS product_name,
            product.price AS product_price,
            product.price_min AS product_price_min,
            product.price_max AS product_price_max
        FROM shopee_shop AS shop
        LEFT JOIN shopee_product AS product
        ON shop.shopid = product.shopid

        WHERE shop.username = :shop_name OR shop.brand_name LIKE :shop_name_pattern

        ORDER BY shop.created_at, product.crawled_at
    )

    SELECT sph.*, p_model.modelid, p_model.name, p_model.price
    FROM shop_product_history AS sph
    LEFT JOIN shopee_product_model AS p_model
    ON sph.itemid = p_model.itemid

    """
    values = {
        "shop_name": shop_name,
        "shop_name_pattern": "%" + shop_name + "%",
        # "product_name": product_name
    }
    result = await database.fetch_all(product_models_stmt, values=values)
    return {"product_name": product_name, "shop_name": shop_name, "r": result}
