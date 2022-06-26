import sqlalchemy as sa
import datetime

from .base import Base


class ShopeeShop(Base):
    __tablename__ = "shopee_shop"

    id = sa.Column(sa.Integer, primary_key=True)
    username = sa.Column(sa.String)
    shopid = sa.Column(sa.Integer)
    brand_name = sa.Column(sa.String)
    shop_created_time = sa.Column(sa.TIMESTAMP)

    created_at = sa.Column(sa.TIMESTAMP, default=datetime.datetime.now)
    updated_at = sa.Column(
        sa.TIMESTAMP,
        default=datetime.datetime.now,
        onupdate=datetime.datetime.now
    )

    __table_args__ = (
        sa.UniqueConstraint('username', 'shopid', name='_username_shopid_uc'),
    )


class ShopeeProduct(Base):
    __tablename__ = "shopee_product"
    id = sa.Column(sa.Integer, primary_key=True)
    itemid = sa.Column(sa.BigInteger)
    shopid = sa.Column(sa.Integer)
    name = sa.Column(sa.String)
    currency = sa.Column(sa.String)
    stock = sa.Column(sa.Integer)
    item_created_time = sa.Column(sa.TIMESTAMP)
    price = sa.Column(sa.Float)
    price_min = sa.Column(sa.FLOAT)
    price_max = sa.Column(sa.FLOAT)
    price_min_before_discount = sa.Column(sa.FLOAT)
    price_max_before_discount = sa.Column(sa.FLOAT)
    discount = sa.Column(sa.Float)

    crawled_at = sa.Column(sa.TIMESTAMP, default=datetime.datetime.now)


class ShopeeProductModel(Base):
    __tablename__ = "shopee_product_model"
    id = sa.Column(sa.Integer, primary_key=True)
    itemid = sa.Column(sa.BigInteger)
    modelid = sa.Column(sa.BigInteger)
    name = sa.Column(sa.String)
    price = sa.Column(sa.Float)

    crawled_at = sa.Column(sa.TIMESTAMP, default=datetime.datetime.now)
