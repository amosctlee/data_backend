import asyncio
import aiohttp
import datetime
import typing
import pandas as pd


class AioShopCrawler:
    def __init__(self) -> None:
        self.brands = []

    async def __call__(self, *args, **kwds) -> pd.DataFrame:
        async def get_all_categories(aioclient):
            async with aioclient.get(
                "https://shopee.tw/api/v4/official_shop/get_categories?tab_type=0"
            ) as resp:

                resp_json = await resp.json()
                categories = resp_json["data"]["categories"]
            return [item for item in categories]

        async def get_shop_by_category_id(aioclient, category_id):
            async with aioclient.get(
                f"https://shopee.tw/api/v4/official_shop/get_shops_by_category?need_zhuyin=1&category_id={category_id}"
            ) as resp:

                resp_json = await resp.json()
                brands = resp_json["data"]["brands"]
                self.brands.extend(
                    [brand for item in brands for brand in item["brand_ids"]]
                )

        async def main():
            async with aiohttp.ClientSession() as session:
                categories = await get_all_categories(session)

                tasks = [
                    get_shop_by_category_id(session, cat["category_id"])
                    for cat in categories
                ]
                await asyncio.gather(*tasks)

        await main()

        df = pd.DataFrame(self.brands)
        df.sort_values(by=["ctime"], inplace=True)
        df.drop_duplicates(['username', 'shopid'], keep="last", inplace=True)
        df.sort_values(by="username", inplace=True)
        df.reset_index(drop=True, inplace=True)
        return df


class AioShopProductsCrawler:
    def __init__(self) -> None:
        self.items = []

    async def __call__(self, shopids: typing.Iterable) -> pd.DataFrame:
        def extract_item_fields(items):
            need_fields = [
                'itemid',
                'shopid',
                'name',
                'currency',
                'stock',
                'status',
                'ctime',
                'sold',
                'historical_sold',
                'liked',
                'liked_count',
                'view_count',
                'catid',
                'brand',
                'item_status',
                'price',
                'price_min',
                'price_max',
                'price_min_before_discount',
                'price_max_before_discount',
                'hidden_price_display',
                'price_before_discount',
                'has_lowest_price_guarantee',
                'show_discount',
                'raw_discount',
                'discount',
            ]
            new_items = []
            for item in items:
                new_item = {key: item[key] for key in need_fields}
                new_items.append(new_item)

            return new_items

        async def get_shop_product(aioclient, shopid, limit=30, offset=0):
            url = "https://shopee.tw/api/v4/recommend/recommend"
            params = {
                "bundle": "shop_page_category_tab_main",
                "item_card": "2",
                "limit": limit,
                "offset": offset,
                "section": "shop_page_category_tab_main_sec",
                "shopid": shopid,
            }
            sorting_choice = [
                {
                    "sort_type": "1",
                    "tab_name": "popular",
                },
                {
                    "sort_type": "2",
                    "tab_name": "latest",
                },
                {
                    "sort_type": "13",
                    "tab_name": "topsale",
                },
            ]
            async with aioclient.get(
                url, params={
                    **params,
                    **sorting_choice[1]
                }
            ) as resp:
                assert resp.status == 200
                resp_json = await resp.json()
                sections = resp_json["data"]["sections"]
                assert len(sections) == 1
                section = sections[0]
                items = section["data"]["item"]

                if items is None:
                    # print( "此賣場尚無商品", "shopid:", shopid, resp_json)
                    pass
                else:
                    self.items.extend(extract_item_fields(items))

                total = section["total"]

            while offset + limit < total:
                offset += limit
                params = {
                    "bundle": "shop_page_category_tab_main",
                    "item_card": "2",
                    "limit": limit,
                    "offset": offset,
                    "section": "shop_page_category_tab_main_sec",
                    "shopid": shopid,
                }
                retry_cnt = 3
                while retry_cnt:
                    async with aioclient.get(
                        url, params={
                            **params,
                            **sorting_choice[1]
                        }
                    ) as resp:
                        assert resp.status == 200
                        resp_json = await resp.json()
                        if resp_json is None:
                            print(
                                "resp_json, 此賣場怪怪", "shopid:", shopid, "limit:",
                                limit, "offset:", offset, resp_json
                            )

                            retry_cnt -= 1
                            continue

                        sections = resp_json["data"]["sections"]
                        assert len(sections) == 1
                        section = sections[0]
                        items = section["data"]["item"]

                        if items is None:
                            print(
                                "items, 此賣場怪怪", "shopid:", shopid, "limit:",
                                limit, "offset:", offset, resp_json
                            )

                            retry_cnt -= 1

                        else:
                            self.items.extend(extract_item_fields(items))

                            retry_cnt = 0

                        total = section["total"]

        async def main():
            nonlocal shopids
            async with aiohttp.ClientSession() as session:
                tasks = [
                    get_shop_product(session, shopid) for shopid in shopids
                ]
                await asyncio.gather(*tasks)

        await main()

        df = pd.DataFrame(self.items)
        df.drop_duplicates(inplace=True)

        return df


class AioProductModelsCrawler:
    def __init__(self) -> None:
        self.item_models = []

    async def __call__(
        self, item_shop_list: typing.List[tuple]
    ) -> pd.DataFrame:
        def extract_model_fields(models):
            need_fields = ['itemid', 'modelid', 'name', 'price']
            new_models = []
            for model in models:
                new_model = {key: model[key] for key in need_fields}
                new_models.append(new_model)
            return new_models

        async def get_item_info(aioclient, itemid, shopid):
            url = "https://shopee.tw/api/v4/item/get"
            params = {
                "itemid": itemid,
                "shopid": shopid,
            }
            async with aioclient.get(url, params=params) as resp:
                resp_json = await resp.json()
                models = resp_json["data"]["models"]
                self.item_models.extend(extract_model_fields(models))

        async def main():
            nonlocal item_shop_list
            async with aiohttp.ClientSession() as session:
                tasks = [
                    get_item_info(session, itemid, shopid)
                    for itemid, shopid in item_shop_list
                ]
                await asyncio.gather(*tasks)

        await main()

        df = pd.DataFrame(self.item_models)
        df.drop_duplicates(inplace=True)

        return df


async def main():
    shops_df = await AioShopCrawler()()
    shops_df.to_csv("./all_shops.csv", index=False)
    # shops_df = pd.read_csv("./all_shops.csv")

    # 只選擇某些店家做 POC
    cond = shops_df['username'] == "google.tw"
    shopids = shops_df[cond].shopid.tolist()

    shop_products_df = await AioShopProductsCrawler()(shopids)

    shop_products_df.to_csv("./all_products.csv", index=False)

    item_shop_list = list(
        zip(
            shop_products_df["itemid"].to_list(),
            shop_products_df["shopid"].to_list()
        )
    )
    product_models_df = await AioProductModelsCrawler()(item_shop_list)
    product_models_df["created_at"] = datetime.datetime.now().isoformat()
    product_models_df.to_csv("./product_models.csv", index=False)


if __name__ == "__main__":
    asyncio.run(main())
