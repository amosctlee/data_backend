import os
import asyncio
import sqlalchemy as sa
import datetime
from sqlalchemy.orm import sessionmaker

import db_model
import crawlers
from settings import settings

engine = sa.create_engine(settings.DB_URI)
Session = sessionmaker(bind=engine)
db_model.Base.metadata.create_all(engine)


class ShopeeRunner:
    # TODO: shop_username only for POC, need to be removed in production.
    async def __call__(self, shop_username):

        shop_df = await self.crawl_shop_to_db()

        cond = shop_df["username"] == shop_username
        shop_products_df = await self.crawl_product_to_db(shop_df[cond])
        assert len(shop_products_df) > 0

        product_models_df = await self.crawl_product_model_to_db(
            shop_products_df
        )

    async def crawl_shop_to_db(self):
        shop_df = await crawlers.shopee.AioShopCrawler()()

        shops = shop_df.to_dict(orient="records")

        with Session() as db:
            indb = db.query(
                db_model.shopee.ShopeeShop.username,
                db_model.shopee.ShopeeShop.shopid
            ).all()
            indb_dict = {(x.username, x.shopid): True for x in indb}

            shop_objs = []
            for shop in shops:
                if (shop["username"], shop["shopid"]) not in indb_dict:
                    shop_obj = db_model.shopee.ShopeeShop()
                    for k in shop:
                        setattr(shop_obj, k, shop[k])
                    shop_obj.shop_created_time = datetime.datetime.fromtimestamp(
                        shop["ctime"]
                    )
                    shop_objs.append(shop_obj)

            db.add_all(shop_objs)
            db.commit()
        return shop_df

    async def crawl_product_to_db(self, shop_df):
        shop_products_df = await crawlers.shopee.AioShopProductsCrawler()(
            shopids=shop_df.shopid.to_list()
        )
        shop_products = shop_products_df.to_dict(orient="records")

        with Session() as db:
            product_objs = []
            for shop_product in shop_products:
                product_obj = db_model.shopee.ShopeeProduct()
                for k in shop_product:
                    if k.startswith("price") and shop_product[k] != -1:
                        setattr(product_obj, k, shop_product[k] / 100000)
                    else:
                        setattr(product_obj, k, shop_product[k])
                product_obj.item_created_time = datetime.datetime.fromtimestamp(
                    shop_product["ctime"]
                )
                product_objs.append(product_obj)

            db.add_all(product_objs)
            db.commit()

        return shop_products_df

    async def crawl_product_model_to_db(self, shop_products_df):
        product_models_df = await crawlers.shopee.AioProductModelsCrawler()(
            item_shop_list=list(
                zip(
                    shop_products_df["itemid"].to_list(),
                    shop_products_df["shopid"].to_list()
                )
            )
        )

        product_models = product_models_df.to_dict(orient="records")

        with Session() as db:
            product_model_objs = []
            for product_model in product_models:
                product_model_obj = db_model.shopee.ShopeeProductModel()
                for k in product_model:
                    if k == "price":
                        setattr(product_model_obj, k, product_model[k] / 100000)
                    else:
                        setattr(product_model_obj, k, product_model[k])

                product_model_obj.crawled_at = datetime.datetime.now()
                product_model_objs.append(product_model_obj)

            db.add_all(product_model_objs)
            db.commit()

        return product_models_df


class MomoRunner:
    def crawl_brand_to_db(self):
        brands_df = crawlers.momo.MomoCrawler().collect_brands()

        brands = brands_df.to_dict(orient="records")

        with Session() as db:
            indb = db.query(
                db_model.momo.MomoBrand.parent_category_code,
                db_model.momo.MomoBrand.parent_category_id
            ).all()

            indb_dict = {
                (x.parent_category_code, x.parent_category_id): True
                for x in indb
            }

            brand_objs = []
            for brand in brands:
                if (
                    brand["parent_category_code"], brand["parent_category_id"]
                ) not in indb_dict:
                    brand_obj = db_model.momo.MomoBrand()
                    for k in brand:
                        setattr(brand_obj, k, brand[k])
                    brand_objs.append(brand_obj)

            db.add_all(brand_objs)
            db.commit()

    # TODO: brand_name only for POC, need to be removed in production.
    def crawl_product_to_db(self, brand_name):
        with Session() as db:
            if brand_name:
                brand_objs = db.query(db_model.momo.MomoBrand).filter(
                    db_model.momo.MomoBrand.child_category_name == brand_name
                ).all()
            else:
                brand_objs = db.query(db_model.momo.MomoBrand).all()

            for brand_obj in brand_objs:
                brand_dict = {
                    c.name: getattr(brand_obj, c.name)
                    for c in db_model.momo.MomoBrand.__table__.c
                }

                if len(brand_dict) == 1:
                    print(brand_obj.id)

                products_df = crawlers.momo.MomoCrawler(
                ).collect_brand_products(brand_dict)

                products = products_df.to_dict(orient="records")

                for product in products:
                    product_obj = db_model.momo.MomoProduct()
                    for k in product:
                        if k == "product_price":
                            setattr(
                                product_obj, "product_price_parsed",
                                int(product[k].replace(",", ""))
                            )
                        setattr(product_obj, k, product[k])

                    db.add(product_obj)
                db.commit()


async def async_crawl():
    runner = ShopeeRunner()
    await runner(shop_username="google.tw")
    await runner(shop_username="microsoft_tw")
    await runner(shop_username="3mofficial")


def crawl():
    momo_runner = MomoRunner()
    momo_runner.crawl_brand_to_db()
    momo_runner.crawl_product_to_db(brand_name="Google")
    momo_runner.crawl_product_to_db(brand_name="Microsoft微軟")
    momo_runner.crawl_product_to_db(brand_name="3M")


if __name__ == "__main__":
    crawl()
    asyncio.run(async_crawl())
