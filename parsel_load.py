import json
import sys
from util import read_json_zip_range
from database import create, batch_insert
from parsel import Selector
import time

path = r"C:\Users\yash.limbasiya\Desktop\Project's\restaurent\Garb_food_restaurent\Grab_food_zpages_60k"
TABLE_NAME = "restaurent"
BATCH_SIZE = 500


def parse(json_data):
    result = {}
    selector = Selector(json.dumps(json_data))
    merchant = selector.jmespath('merchant')

    result["restaurant_id"] = merchant.jmespath("ID").get()
    result["restaurant_name"]= merchant.jmespath("name").get()
    result["restaurant_cuisine"]= merchant.jmespath("cuisine").get()
    result["restaurant_image"]  = merchant.jmespath("photoHref").get()
    result["restaurant_timezone"]= merchant.jmespath("ETA").get()
    result["restaurant_opening_hours"] = json.dumps(merchant.jmespath("openingHours").get())

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

    result['restaurant_menu'] = json.dumps(restaurent_menu)
    return result


def main(start_index, end_index):
    create(TABLE_NAME)

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

    print(f"[{start_index}-{end_index}] Done! Total rows inserted: {total}")


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python parsel_load.py <start> <end>")
        sys.exit(1)

    start = int(sys.argv[1])
    end   = int(sys.argv[2])

    st = time.time()
    main(start, end)
    tt = time.time() - st
    print(f"Total time: {tt:.2f} seconds")



    