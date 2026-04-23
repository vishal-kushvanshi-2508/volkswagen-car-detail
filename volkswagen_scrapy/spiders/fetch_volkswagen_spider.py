import scrapy
from urllib.parse import urlencode
import json

class FetchVolkswagenSpiderSpider(scrapy.Spider):
    name = "fetch_volkswagen_spider"
    # allowed_domains = ["www.volkswagen.co.uk"]
    # start_urls = ["https://www.volkswagen.co.uk/en/new/available-stock-locator/gsl-plp.html"]




    headers = {
        'accept': '*/*',
        'accept-language': 'en-US,en;q=0.9',
        'cache-control': 'no-cache',
        'origin': 'https://www.volkswagen.co.uk',
        'pragma': 'no-cache',
        'priority': 'u=1, i',
        'referer': 'https://www.volkswagen.co.uk/',
        'sec-ch-ua': '"Google Chrome";v="147", "Not.A/Brand";v="8", "Chromium";v="147"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'cross-site',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/147.0.0.0 Safari/537.36',
    }

    params = {
        't_manuf': 'BQ',
        't_reserved': 'false',
        'sort': 'DATE_OFFER',
        'sortdirection': 'ASC',
        'pageitems': '12',
        'page': f"{1}",
        'country': 'GB',
        'endpoint': '{"endpoint":{"type":"publish","country":"gb","language":"en","content":"onehub_pkw","envName":"prod","testScenarioId":null},"signature":"ZWQcGYMemGx1hkFO4sq/vKJYYBJSVGPXQR7rMDs6BpE="}',
        'language': 'en',
        'market': 'passenger',
        'oneapiKey': 'nOqkwPxxu8ViK9aaHvTkglzVZAlX4yIx',
        'dataVersion': '640B4505742CCF6F343556C288EFEC22',
    }


    def start_requests(self):
            # rows = self.fetch_all_movies()
            base_url = "https://v3-111-2.gsl.feature-app.io/bff/car/search"
            url = f"{base_url}?{urlencode(self.params)}"

            yield scrapy.Request(
                url=url,
                headers=self.headers,
                callback=self.parse,
                # meta={"row": row},
                dont_filter=True
            )
            


    def parse(self, response):

        print("---------parse-----------")

        with open("car_link.json", "w") as f:
             json.dump(response.json(), f, indent=4)
             
        python_dict = response.json()

        current_page = python_dict.get("meta", {}).get("page", "")

        car_list = python_dict.get("cars", [])

        #  STOP when empty list
        if not car_list:
            print("No more cars found. Stopping spider.")
            return        

        for dict_data in car_list:
            car_name = dict_data.get("title", "")
            car_stock_link = dict_data.get("stockLinks", {}).get("vwukgsl", {}).get("value", "")
            car_unique_key = dict_data.get("key", "")
            yield {
                "type" : "car_urls",
                "car_name": car_name,
                "car_link": car_stock_link,
                "unique_key": car_unique_key,
                "status" : "pending"
            }


        #  Next page
        next_page = current_page + 1

        #  Use copy (important)
        new_params = self.params.copy()
        new_params["page"] = str(next_page)

        base_url = "https://v3-111-2.gsl.feature-app.io/bff/car/search"
        next_url = f"{base_url}?{urlencode(new_params)}"

        print("Next Page:", next_page)

        yield scrapy.Request(
            url=next_url,
            headers=self.headers,
            callback=self.parse,
            dont_filter=True
        )

