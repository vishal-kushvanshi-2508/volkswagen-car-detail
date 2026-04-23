# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
import mysql.connector
from mysql.connector import Error



class VolkswagenScrapyPipeline:

    def __init__(self):
        # Database configuration
        self.host = "localhost"
        self.user = "root"
        self.password = "actowiz"  # replace with your MySQL password
        self.port = "3306"
        self.database = "volkswagen_scrapy_db_second"

    def open_spider(self, spider):
        """Runs when spider starts"""
        try:
            # Connect to MySQL server
            self.conn = mysql.connector.connect(
                host=self.host,
                user=self.user,
                password=self.password,
                port=self.port
            )
            self.cursor = self.conn.cursor()

            # Create database if not exists
            self.cursor.execute(f"CREATE DATABASE IF NOT EXISTS {self.database}")
            self.conn.database = self.database


            # ================================
            #  1. Create all_category table
            # ================================
            self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS car_urls (
                id INT AUTO_INCREMENT PRIMARY KEY,
                car_name VARCHAR(255),
                car_link TEXT,
                unique_key VARCHAR(255),
                status VARCHAR(50) DEFAULT 'pending'
            )
            """)

            # # ==================================
            # # 2. Create product_api table
            # # ==================================
            self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS car_details (
                id INT AUTO_INCREMENT PRIMARY KEY,
                total_price VARCHAR(200),
                finance_rate VARCHAR(200),
                deliveryInfo TEXT,
                retailer TEXT,
                images JSON,
                vehicle_overview JSON,
                vehicle_details JSON,
                standard_equipment JSON
            )
            """)

            self.conn.commit()
        except Error as e:
            spider.logger.error(f"Error connecting to MySQL: {e}")


    def process_item(self, item, spider):
        print("---process_item---", item)

        # -------------------------------
        # Insert into all_category
        # -------------------------------
        if item.get("type") == "car_urls":
            query = """
            INSERT INTO car_urls (car_name, car_link, unique_key, status)
            VALUES (%s, %s, %s, %s)
            """

            values = (
                item.get("car_name"),
                item.get("car_link"),
                item.get("unique_key"),
                item.get("status", "pending")
            )

            self.cursor.execute(query, values)
            self.conn.commit()

        # -------------------------------
        #  Insert into product_api
        # -------------------------------
        elif item.get("type") == "car_details":
            query = """
            INSERT INTO car_details (total_price, finance_rate, deliveryInfo, retailer, images, vehicle_overview, vehicle_details, standard_equipment)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """

            values = (
                item.get("total_price"),
                item.get("finance_rate"),
                item.get("deliveryInfo"),
                item.get("retailer"),
                item.get("images"),
                item.get("vehicle_overview"),
                item.get("vehicle_details"),
                item.get("standard_equipment")
            )

            self.cursor.execute(query, values)
            self.conn.commit()

        return item





    # ====================================
    #  Close Connection
    # ====================================
    def close_spider(self, spider):
        self.cursor.close()
        self.conn.close()