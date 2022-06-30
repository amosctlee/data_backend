from typing import Optional, List
from fastapi import FastAPI, status
from pydantic import BaseModel, Field
from databases import Database
import asyncio

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


shopee_url_template = "https://shopee.tw/{}-i.{}.{}"  # name, shopid, itemid
momo_url_template = "https://m.momoshop.com.tw{}"  # product_url_path


class ProductOut(BaseModel):
    name: str
    price: float
    url: str
    id_: int = Field(..., alias="id")
    platform: str


def deduplicate_keep_first(items) -> list:
    seen = set()
    result = []
    for item in items:
        if not item["url"] in seen:
            seen.add(item["url"])
            result.append(item)
    return result


@app.get("/product/", response_model=List[ProductOut])
async def search_product(product_name: str) -> List[ProductOut]:

    shopee_stmt = """
    SELECT * FROM public.shopee_product
    WHERE name LIKE :product_name_pattern
    ORDER BY crawled_at DESC LIMIT 10
    """

    momo_stmt = """
    SELECT * from momo_product
    WHERE product_name LIKE :product_name_pattern
    ORDER BY crawled_at DESC LIMIT 10
    """

    values = {
        "product_name_pattern":
            "%{}%".format("%".join(product_name.strip().split(" ")))
    }

    shopee_result, momo_result = await asyncio.gather(
        database.fetch_all(shopee_stmt, values=values),
        database.fetch_all(momo_stmt, values=values)
    )

    shopee_products = [
        {
            "platform":
                "shopee",
            "id":
                row["id"],
            "name":
                row["name"],
            "price":
                row["price"],
            "url":
                shopee_url_template.format(
                    row["name"], row["shopid"], row["itemid"]
                ),
        } for row in shopee_result
    ]

    momo_products = [
        {
            "platform": "momo",
            "id": row["id"],
            "name": row["product_name"],
            "price": row["product_price_parsed"],
            "url": momo_url_template.format(row["product_url_path"]),
        } for row in momo_result
    ]

    mix = shopee_products + momo_products
    mix.sort(key=lambda x: x["id"], reverse=True)  # let the latest one be first
    mix.sort(key=lambda x: x["price"], reverse=False)

    return deduplicate_keep_first(mix)
