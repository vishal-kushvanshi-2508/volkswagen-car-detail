import scrapy
import mysql.connector
import json
from urllib.parse import urlencode

from volkswagen_scrapy import items




class VolkswagenDetailSpiderSpider(scrapy.Spider):
    name = "volkswagen_detail_spider"
    # allowed_domains = ["v3-111-2.gsl.feature-app.io"]
    # start_urls = ["https://v3-111-2.gsl.feature-app.io/bff/car/get"]

    # ---------- DB ----------
    def fetch_all_movies(self):
        connection = mysql.connector.connect(
            host="localhost",
            user="root",
            password="actowiz",
            database="volkswagen_scrapy_db_second"
        )

        cursor = connection.cursor(dictionary=True)
        cursor.execute("SELECT * FROM car_urls WHERE status='pending'")
        rows = cursor.fetchall()

        cursor.close()
        connection.close()
        return rows

    def update_status(self, car_id, status):
        connection = mysql.connector.connect(
            host="localhost",
            user="root",
            password="actowiz",
            database="volkswagen_scrapy_db_second"
        )
        cursor = connection.cursor()
        cursor.execute(
            "UPDATE car_urls SET status=%s WHERE id=%s",
            (status, car_id)
        )
        connection.commit()
        cursor.close()
        connection.close()



    def errback_handler(self, failure):
        request = failure.request
        car_id = request.meta.get("car_id")
        retry_count = request.meta.get("retry_count", 0)

        if retry_count < 3:
            self.logger.warning(f"Errback retry {retry_count+1} car_id={car_id}")

            yield scrapy.Request(
                url=request.url,
                headers=request.headers,
                callback=self.parse,
                errback=self.errback_handler,
                meta={
                    "car_id": car_id,
                    "retry_count": retry_count + 1
                },
                dont_filter=True
            )
        else:
            self.logger.error(f"Final FAIL (network) car_id={car_id}")
            self.update_status(car_id, "failed")

    # ---------- START ----------

    def start_requests(self):
        rows = self.fetch_all_movies()

        for row in rows:
            unique_key=row["unique_key"]
            car_id = row["id"]

            base_url  = "https://v3-111-2.gsl.feature-app.io/bff/car/get"

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
                'key': f"{unique_key}",
                'country': 'GB',
                'endpoint': '{"endpoint":{"type":"publish","country":"gb","language":"en","content":"onehub_pkw","envName":"prod","testScenarioId":null},"signature":"ZWQcGYMemGx1hkFO4sq/vKJYYBJSVGPXQR7rMDs6BpE="}',
                'language': 'en',
                'market': 'passenger',
                'oneapiKey': 'nOqkwPxxu8ViK9aaHvTkglzVZAlX4yIx',
                'dataVersion': '640B4505742CCF6F343556C288EFEC22',
            }

            full_url = f"{base_url}?{urlencode(params)}"

            yield scrapy.Request(
                url=full_url,
                headers=headers,
                callback=self.parse,
                errback=self.errback_handler,
                meta={
                    "car_id": car_id,
                    "retry_count": 0,
                    "unique_key": unique_key
                },
                dont_filter=True
            )

    def parse(self, response):
        # print("---------detail parse-----------")
        
        car_id = response.meta["car_id"]
        retry_count = response.meta.get("retry_count", 0)
        unique_key = response.meta.get("unique_key")

        # ---------------- RETRY LOGIC ----------------
        if response.status == 500:
            if retry_count < 3:
                self.logger.warning(f"Retry {retry_count+1} for car_id={car_id}")

                yield scrapy.Request(
                    url=response.url,
                    headers=response.request.headers,
                    callback=self.parse,
                    errback=self.errback_handler,
                    meta={
                        "car_id": car_id,
                        "retry_count": retry_count + 1,
                        "unique_key": unique_key
                    },
                    dont_filter=True
                )
            else:
                self.logger.error(f"FAILED after retries car_id={car_id}")
                self.update_status(car_id, "failed")

            return
        
        if response.status != 200:
            self.logger.error(f"Failed: {response.status}")
            self.logger.error(response.text)
            return
        
        with open("car_price_detail.json", "w") as f:
             json.dump(response.json(), f, indent=4)
             
        python_dict = response.json()

        # -----------------total_price---------

        # adjust path based on actual response structure
        total_price = python_dict.get("parsedPrice", {}).get("label", "")

        finance_rate = python_dict.get("hypermediaFinancing", {}).get("default", {}).get("Rate", "")    
        deliveryInfo = python_dict.get("deliveryInfo", {}).get("value", "")

        contact  = python_dict.get("contactData", {})

        parts = [
            contact.get("dealerLabel"),
            contact.get("dealerStreet"),
            contact.get("dealerAddress"),
        ]

        retailer = ", ".join([p.strip() for p in parts if p])

        ## ---------images------------ 
        images = python_dict.get("images", [])

        image_list = []

        for img in images:
            image_data = {
                "key": img.get("key"),
                "main_image_url": img.get("href"),
                "srcset": img.get("srcSet", [])
            }

            image_list.append(image_data)


        # ----------overview------------
        # engin data 
        fuel = python_dict.get("motor", {}).get("fuel", {}).get("value", "")
        power_value = python_dict.get("motor", {}).get("powerKw", {}).get("value", "")
        power_unit = python_dict.get("motor", {}).get("powerKw", {}).get("unit", "")

        engine = f"{fuel} | {power_value} {power_unit}".strip()

        
        gear = python_dict.get("gear", {}).get("value", "")
        color = python_dict.get("color", {}).get("out", {}).get("value", "")    

        result = {}
        engine_data = python_dict.get("hypermediaTechData", {}).get("EngineDataBusiness", {})

        # -------------------------
        # Fuel Consumption
        # -------------------------
        consumption = engine_data.get("consumption", {}).get("data", [])

        for item in consumption:
            for lvl1 in item.get("values", []):  # WLTP
                for lvl2 in lvl1.get("values", []):  # PETROL
                    for entry in lvl2.get("values", []):
                        name = entry.get("name")  # Low, Medium, etc.
                        value = entry.get("value")
                        unit = entry.get("unit")

                        if name and value and unit:
                            key = f"Fuel consumption {name}"
                            result[key] = f"{value} {unit}"

        # CO2 Emissions
        emission = engine_data.get("emission", {}).get("data", [])

        for item in emission:
            for lvl1 in item.get("values", []):  # WLTP
                for lvl2 in lvl1.get("values", []):  # PETROL
                    for entry in lvl2.get("values", []):
                        name = entry.get("name")  # combined
                        value = entry.get("value")
                        unit = entry.get("unit")

                        if name and value and unit:
                            key = f"CO₂-Emissions {name}"
                            result[key] = f"{value} {unit}"

        overview = {
            "Engine": engine,
            "Gear": gear,
            "color": color,
            "Vehicle emission": result
        }
        # print(overview)



        # --------------------Vehicle data------------
        # -------- Engine --------
        fuel = python_dict.get("motor",{}).get("fuel", {}).get("value", "")
        power_val = python_dict.get("motor", {}).get("powerKw", {}).get("value", "")
        power_unit = python_dict.get("motor", {}).get("powerKw", {}).get("unit", "")
        capacity_val = python_dict.get("motor", {}).get("capacity", {}).get("value", "")
        capacity_unit = python_dict.get("motor", {}).get("capacity", {}).get("unit", "")
        # capacity_unit = python_dict.get("motor", {}).get("engineCapacity", {}).get("unit", "")

        engine_parts = []

        if fuel:
            engine_parts.append(fuel)

        if power_val and power_unit:
            engine_parts.append(f"{power_val} {power_unit}")

        if capacity_val and capacity_unit:
            # format 1498 → 1,498
            try:
                capacity_val = f"{int(capacity_val):,}"
            except:
                pass
            engine_parts.append(f"{capacity_val} {capacity_unit}")

        engine = " | ".join(engine_parts)

        # -------- Final Dictionary --------
        vehicle_python_dict = {
            "Engine": engine,
            "Model year": python_dict.get("modelyear", {}).get("value"),
            "Drive type": python_dict.get("drive", {}).get("value"),
            "Gear type": python_dict.get("gear", {}).get("value"),
            "Exterior colour": python_dict.get("color", {}).get("out", {}).get("value"),
            "Vehicle emission": result,
            "noiseLevel" : python_dict.get("noiseLevel", {})
        }


        ## -------------stander equipment------------
        standard_equipment = {}

        equipmentTabs = python_dict.get("equipmentTabs", [])# [0].get("items", [])

        if equipmentTabs:
            items = equipmentTabs[2].get("items", [])

            for item in items:
                headline = item.get("headline")
                values = item.get("values", [])

                texts = []

                for val in values:
                    text = val.get("text")

                    if text:
                        # clean unwanted characters like \xa0
                        clean_text = text.replace("\xa0", " ").strip()
                        texts.append(clean_text)

                if headline and texts:
                    standard_equipment[headline] = texts

        yield {
            "type" : "car_details",
            "total_price": total_price,
            "finance_rate": finance_rate,
            "deliveryInfo": deliveryInfo,
            "retailer": retailer,
            "images": json.dumps(image_list),
            "vehicle_overview": json.dumps(overview),
            "vehicle_details": json.dumps(vehicle_python_dict),
            "standard_equipment": json.dumps(standard_equipment)
        }
        
        self.update_status(car_id, "success")