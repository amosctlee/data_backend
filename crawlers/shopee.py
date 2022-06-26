import asyncio
import aiohttp
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
        df.drop_duplicates(inplace=True)
        df.sort_values(by="username", inplace=True)

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


if __name__ == "__main__":
    shops_df = asyncio.run(AioShopCrawler()())
    # shops_df.to_csv("./all_shops.csv")
    # shops_df = pd.read_csv("./all_shops.csv")

    # 只選擇某些店家做 POC
    cond = shops_df['username'] == "google.tw"
    shopids = shops_df[cond].shopid.tolist()

    shop_products_df = asyncio.run(AioShopProductsCrawler()(shopids))

    shop_products_df.to_csv("./all_products.csv")
