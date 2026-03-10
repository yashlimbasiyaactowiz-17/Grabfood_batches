import json
import logging 
from threading import Thread
from util import read_json_zip_range
from database import create, batch_insert
from parsel import Selector


path = r"C:\Users\yash.limbasiya\Desktop\Projects\restaurent\Garb_food_restaurent\Grab_food_zpages_60k"
# path = r"C:\Users\yash.limbasiya\Desktop\Projects\restaurent\Garb_food_restaurent\demo"
TABLE_NAME = "restaurent"
BATCH_SIZE = 2000

# loggin details 
logging.basicConfig(level=logging.INFO,
                    filename='app.log',
                    encoding='utf-8',
                    format='%(asctime)s - %(levelname)s - %(message)s')

# parsel errors alag file me jayenge
_parsel_error_handler = logging.FileHandler("parsel_error.log", encoding='utf-8')
_parsel_error_handler.setLevel(logging.ERROR)
_parsel_error_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
logging.getLogger().addHandler(_parsel_error_handler)

def parse(json_data):
    result = {}
    try:
        selector = Selector(json.dumps(json_data))
        merchant = selector.jmespath('merchant')

        result["restaurant_id"] = merchant.jmespath("ID").get()
        result["restaurant_name"]= merchant.jmespath("name").get()
        result["restaurant_cuisine"]= merchant.jmespath("cuisine").get()
        result["restaurant_image"]  = merchant.jmespath("photoHref").get()
        result["restaurant_timezone"]= merchant.jmespath("ETA").get()
        result["restaurant_opening_hours"] = json.dumps(merchant.jmespath("openingHours").get(), ensure_ascii=False)

        restaurent_menu = []
        
        categories = merchant.jmespath("menu.categories")

        for category in categories:
            menu_data = {}
            menu_data['category_id'] = category.jmespath("ID").get()
            menu_data['category_name'] = category.jmespath("name").get()
            menu_data['category_is_available'] = category.jmespath("available").get()
            menu_data['category_items'] = []

            for items in category.jmespath("items"):
                items_data = {}
                items_data['item_id'] = items.jmespath("ID").get()
                items_data['item_name'] = items.jmespath("name").get()
                items_data['item_description'] = items.jmespath("discription").get()
                items_data['item_is_available'] = items.jmespath("available").get()
                items_data['item_photo'] = items.jmespath("imgHref").get()
                items_data['item_price'] = items.jmespath('takeawayPriceInMin').get()
                items_data['item_discounted_price'] = items.jmespath('discountedTakeawayPriceInMin').get()
                menu_data['category_items'].append(items_data)

            restaurent_menu.append(menu_data)

        result["restaurant_menu"] = json.dumps(restaurent_menu, ensure_ascii=False)
    except Exception as e:
        logging.error(f"Error in parse(): {e}", exc_info=True)
    return result


def main(start_index, end_index):
    batch = []
    total = 0

    for raw in read_json_zip_range(path, start_index, end_index):
        result = parse(raw)
        batch.append(result)

        if len(batch) >= BATCH_SIZE:
            batch_insert(table_name=TABLE_NAME, rows=batch)
            total += len(batch)
            batch = []

    if batch:   
        batch_insert(table_name=TABLE_NAME, rows=batch)
        total += len(batch)

    logging.info(f"[{start_index}-{end_index}] Done! Total rows inserted: {total}")


if __name__ == "__main__":
    logging.info("script started")
    create(table_name=TABLE_NAME)
    
    #use Thread's
    threads = []
    step = 10000
    total_files = 60000

    for start in range(0, total_files, step):
        end = start + step
        logging.info(f"start: {start}, end: {end}")
        target_obj = Thread(target=main, args=(start, end))
        threads.append(target_obj)
        target_obj.start()

    for t in threads:
        t.join()
    logging.info("script completed")