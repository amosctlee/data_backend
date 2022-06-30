import requests
import pandas as pd
import datetime
from pathlib import Path
from bs4 import BeautifulSoup
import typing
import asyncio
import aiohttp


class MomoDownloader:
    def get_all_parentCategoryCode_of_brands(self):

        headers = {
            'authority':
                'm.momoshop.com.tw',
            'origin':
                'https://m.momoshop.com.tw',
            'user-agent':
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.0.0 Safari/537.36',
        }

        shop_url = "https://m.momoshop.com.tw/ajax/ajaxTool.jsp"
        params = {
            "n": "getMenuNew",
            "t": int(datetime.datetime.now().timestamp() * 1000)
        }
        data = {
            'data':
                '{"flag":"getMenuNew","data":{"cn":"brandAll","subId":"brandAll"}}',
        }

        resp = requests.post(
            shop_url,
            params=params,
            data=data,
            headers=headers,
        )
        extracted_items = []
        for item in resp.json()["rtnData"]["cateGoodsHM"]["parentCategories"]:
            if item["parentCategoryCode"] == "brandAll":
                # 跳過自己
                continue
            extracted_items.append(
                {
                    "parentCategoryCode": item["parentCategoryCode"],
                    "parentCategoryName": item["parentCategoryName"],
                    "parentCategoryId": item["parentCategoryId"],
                }
            )
        return extracted_items

    def get_childCategoryCode_of_category_brand(
        self, parentCategoryCode, parentCategoryId
    ) -> typing.Tuple[typing.List[dict], typing.List[dict]]:
        parentCategoryCode = str(parentCategoryCode)
        parentCategoryId = str(parentCategoryId)

        headers = {
            'authority':
                'm.momoshop.com.tw',
            'origin':
                'https://m.momoshop.com.tw',
            'user-agent':
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.0.0 Safari/537.36',
        }

        shop_url = "https://m.momoshop.com.tw/ajax/ajaxTool.jsp"
        params = {
            "n": "getMenuNew",
            "t": int(datetime.datetime.now().timestamp() * 1000)
        }
        data = {
            'data':
                '{"flag":"getMenuNew","data":{"cn":"' + parentCategoryCode +
                '","subId":"' + parentCategoryId + '"}}',
        }

        resp = requests.post(
            shop_url,
            params=params,
            data=data,
            headers=headers,
        )

        resp_json = resp.json()

        child_categories = []
        if "childCategories" in resp_json["rtnData"]["cateGoodsHM"]:
            for child_category in resp_json["rtnData"]['cateGoodsHM'][
                'childCategories']:
                categoryTitle = child_category["categoryTitle"]

                for brand in child_category["childCategoriesInfo"]:
                    # if brand["childCategoryCode"] == '':
                    #     print(brand)
                    child_categories.append(
                        {
                            "categoryTitle": categoryTitle,
                            "childCategoryCode": brand["childCategoryCode"],
                            "childCategoryName": brand["childCategoryName"],
                        }
                    )
        extra_child_categories = []
        if "extraChildCategories" in resp_json["rtnData"]["cateGoodsHM"]:
            for extra_child_category in resp_json["rtnData"]["cateGoodsHM"][
                "extraChildCategories"]:
                categoryTitle = extra_child_category["categoryTitle"]

                for brand in extra_child_category["childCategoriesInfo"]:
                    # if brand["childCategoryCode"] == '':
                    #     print(brand)
                    extra_child_categories.append(
                        {
                            "categoryTitle": categoryTitle,
                            "childCategoryCode": brand["childCategoryCode"],
                            "childCategoryName": brand["childCategoryName"],
                        }
                    )

        return child_categories, extra_child_categories

    def get_brand_product_page(self, childCategoryCode):
        params = {
            'cn': childCategoryCode,
            'cid': 'dir',
            'oid': 'dir',
            'imgSH': 'fourCardStyle',
        }

        headers = {
            'authority':
                'm.momoshop.com.tw',
            'origin':
                'https://m.momoshop.com.tw',
            'user-agent':
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.0.0 Safari/537.36',
        }

        response = requests.get(
            'https://m.momoshop.com.tw/category.momo',
            params=params,
            headers=headers
        )

        return response

    def get_brand_product_page_n(self, childCategoryCode, n):
        params = {
            'cn': childCategoryCode,
            'page': n,
            'sortType': '6',
            'imgSH': 'fourCardStyle',
        }

        headers = {
            'authority':
                'm.momoshop.com.tw',
            'origin':
                'https://m.momoshop.com.tw',
            'user-agent':
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.0.0 Safari/537.36',
        }

        response = requests.get(
            'https://m.momoshop.com.tw/category.momo',
            params=params,
            headers=headers
        )
        return response


class AioMomoBrandProductCrawler:
    def __init__(self):
        self.products = []

    async def __call__(self, childCategoryCode_list):
        raise Exception("will be banned")

        async def crawl_page_1(aioclient, childCategoryCode):
            params = {
                'cn': childCategoryCode,
                'cid': 'dir',
                'oid': 'dir',
                'imgSH': 'fourCardStyle',
            }

            headers = {
                'authority':
                    'm.momoshop.com.tw',
                'origin':
                    'https://m.momoshop.com.tw',
                'user-agent':
                    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.0.0 Safari/537.36',
            }
            url = 'https://m.momoshop.com.tw/category.momo'

            async with aioclient.get(
                url, params=params, headers=headers
            ) as resp:
                resp = await resp.text()

            return resp

        async def crawl_page_n(aioclient, childCategoryCode, n):
            params = {
                'cn': childCategoryCode,
                'page': n,
                'sortType': '6',
                'imgSH': 'fourCardStyle',
            }

            headers = {
                'authority':
                    'm.momoshop.com.tw',
                'origin':
                    'https://m.momoshop.com.tw',
                'user-agent':
                    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.0.0 Safari/537.36',
            }

            url = 'https://m.momoshop.com.tw/category.momo'
            async with aioclient.get(
                url, params=params, headers=headers
            ) as resp:
                resp = await resp.text()
            return resp

        async def crawl_and_parse(aioclient, childCategoryCode):
            all_product_list = []

            n = 1
            while True:
                if n == 1:
                    response = await crawl_page_1(aioclient, childCategoryCode)
                else:
                    response = await crawl_page_n(
                        aioclient, childCategoryCode, n
                    )

                product_list = MomoProductPageParser().parse_products(response)

                all_product_list.extend(product_list)

                if len(product_list) == 0:
                    break
                n += 1

            self.products.append((childCategoryCode, all_product_list))

        async def main():
            nonlocal childCategoryCode_list
            async with aiohttp.ClientSession() as session:
                tasks = [
                    crawl_and_parse(session, childCategoryCode)
                    for childCategoryCode in childCategoryCode_list
                ]
                await asyncio.gather(*tasks)

        await main()

        df = pd.DataFrame(self.products)

        return df


class MomoProductPageParser:
    def parse_products(self, text) -> typing.List[dict]:
        soup = BeautifulSoup(text, 'html.parser')
        article = soup.find("article", class_="prdListArea")
        if article is None:
            return []

        article_li_list = article.find_all("li")

        product_list = []
        for i, li in enumerate(article_li_list):
            product_url_path = li.find("a", class_="productInfo")["href"]
            product_event = li.find("p", class_="prdEvent").text.strip()
            product_name = li.find("h3", class_="prdName").text.strip()
            product_price = li.find("b", class_="price").text.strip()
            product_price_text = li.find("b", class_="priceText").text.strip()

            item = {
                "product_url_path": product_url_path,
                "product_event": product_event,
                "product_name": product_name,
                "product_price": product_price,
                "product_price_text": product_price_text,
            }
            product_list.append(item)
        return product_list


class MomoCrawler:
    def __init__(self):
        self.momo = MomoDownloader()
        self.momo_product_page = MomoProductPageParser()

    def collect_brand_products(self, brand: dict):

        all_product_list = []

        n = 1
        while True:

            if brand["child_category_code"] == "":
                break

            response = self.momo.get_brand_product_page_n(
                brand["child_category_code"], n
            )
            product_list = self.momo_product_page.parse_products(response.text)

            all_product_list.extend(product_list)

            if len(product_list) == 0:
                break
            n += 1

        for item in all_product_list:
            item["child_category_code"] = brand["child_category_code"]
            item["child_category_name"] = brand["child_category_name"]

        df = pd.DataFrame(all_product_list)
        df.drop_duplicates(inplace=True)

        return df

    def collect_brands(self) -> typing.List[dict]:
        brands = []
        parent_category_list = self.momo.get_all_parentCategoryCode_of_brands()

        for parent_category in parent_category_list:
            parentCategoryCode = parent_category["parentCategoryCode"]
            parentCategoryName = parent_category["parentCategoryName"]
            parentCategoryId = parent_category["parentCategoryId"]

            child_category_list, extra_child_category_list = self.momo.get_childCategoryCode_of_category_brand(
                parent_category["parentCategoryCode"],
                parent_category["parentCategoryId"]
            )

            for child_category in child_category_list:
                categoryTitle = child_category["categoryTitle"]
                childCategoryCode = child_category["childCategoryCode"]
                childCategoryName = child_category["childCategoryName"]

                brands.append(
                    {
                        "parent_category_code": parentCategoryCode,
                        "parent_category_name": parentCategoryName,
                        "parent_category_id": parentCategoryId,
                        "category_title": categoryTitle,
                        "child_category_code": childCategoryCode,
                        "child_category_name": childCategoryName,
                    }
                )

            for extra_child_category in extra_child_category_list:
                categoryTitle = extra_child_category["categoryTitle"]
                childCategoryCode = extra_child_category["childCategoryCode"]
                childCategoryName = extra_child_category["childCategoryName"]

                brands.append(
                    {
                        "parent_category_code": parentCategoryCode,
                        "parent_category_name": parentCategoryName,
                        "parent_category_id": parentCategoryId,
                        "category_title": categoryTitle,
                        "child_category_code": childCategoryCode,
                        "child_category_name": childCategoryName,
                    }
                )

        df = pd.DataFrame(brands)
        df.drop_duplicates(
            subset=[
                "parent_category_code",
                "parent_category_id",
                "child_category_code",
            ],
            keep="last",
            inplace=True
        )

        return df


if __name__ == "__main__":
    brands_df = MomoCrawler().collect_brands()

    print(brands_df.shape)

    brands = brands_df.to_dict("records")
    for brand in brands:
        products_df = MomoCrawler().collect_brand_products(brand)

        print(products_df.shape)