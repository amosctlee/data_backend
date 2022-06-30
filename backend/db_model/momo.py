import sqlalchemy as sa
import datetime

from .base import Base


class MomoBrand(Base):
    __tablename__ = "momo_brand"

    id = sa.Column(sa.Integer, primary_key=True)

    parent_category_code = sa.Column(sa.String)
    parent_category_name = sa.Column(sa.String)
    parent_category_id = sa.Column(sa.String)
    category_title = sa.Column(sa.String)
    child_category_code = sa.Column(sa.String)
    child_category_name = sa.Column(sa.String)

    created_at = sa.Column(sa.TIMESTAMP, default=datetime.datetime.now)
    updated_at = sa.Column(
        sa.TIMESTAMP,
        default=datetime.datetime.now,
        onupdate=datetime.datetime.now
    )

    __table_args__ = (
        sa.UniqueConstraint(
            'parent_category_code',
            'parent_category_id',
            'child_category_code',
            name='momo_brand_uc'
        ),
        sa.Index('momo_brand_parent_category_code_idx', 'parent_category_code'),
        sa.Index('momo_brand_parent_category_id_idx', 'parent_category_id'),
        sa.Index('momo_brand_parent_category_name_idx', 'parent_category_name'),
        sa.Index('momo_brand_child_category_code_idx', 'child_category_code'),
        sa.Index('momo_brand_child_category_name_idx', 'child_category_name'),
    )


class MomoProduct(Base):
    __tablename__ = "momo_product"

    id = sa.Column(sa.Integer, primary_key=True)

    child_category_code = sa.Column(sa.String)
    child_category_name = sa.Column(sa.String)

    product_url_path = sa.Column(sa.String)
    product_event = sa.Column(sa.String)
    product_name = sa.Column(sa.String)
    product_price = sa.Column(sa.String)
    product_price_parsed = sa.Column(sa.Float)
    product_price_text = sa.Column(sa.String)

    crawled_at = sa.Column(sa.TIMESTAMP, default=datetime.datetime.now)

    __table_args__ = (
        sa.Index('momo_product_product_name_idx', 'product_name'),
        sa.Index('momo_product_product_price_idx', 'product_price'),
        sa.Index('momo_product_product_price_text_idx', 'product_price_text'),
        sa.Index('momo_product_crawled_at_idx', 'crawled_at'),
    )