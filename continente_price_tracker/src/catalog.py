import requests
from bs4 import BeautifulSoup
import json
import pandas as pd
import re
import time
import random
from datetime import datetime
from utils import retry_on_failure


def parse_total_products(html_content):
    # Parse the HTML content
    soup = BeautifulSoup(html_content, 'html.parser')

    # Find the div with the specific class
    counter_div = soup.find(
        "div",
        class_="search-results-products-counter d-flex justify-content-center")

    # Check if the div is found and contains text
    if counter_div and counter_div.text:
        # Extract numbers from the text using regex
        numbers = re.findall(r'\d+', counter_div.text)

        # Convert numbers to integers and get the max value (total number of products)
        if numbers:
            number_products = max(map(int, numbers))
            return number_products

    # Return None if no number is found
    return None


def parse_product_data(html_content, cgid):
    # Parse the HTML content
    soup = BeautifulSoup(html_content, 'html.parser')

    # Find all product tiles
    product_tiles = soup.find_all("div", class_="product-tile")

    # Initialize lists to store product data
    product_data = []

    # Loop through each product tile and extract data
    for tile in product_tiles:
        # Extract data from data-product-tile-impression JSON
        product_info_json = tile.get("data-product-tile-impression")
        if product_info_json:
            try:
                # Replace unescaped single quotes with escaped single quotes
                product_info_json = product_info_json.replace("'", "\\'")
                product_info = json.loads(product_info_json)

                # Get product details from the JSON object
                name = product_info.get("name", "")
                product_id = product_info.get("id", "")
                price_per_kg = product_info.get("price", 0.0)
                brand = product_info.get("brand", "")
                category = product_info.get("category", "")

            except json.JSONDecodeError:
                print(f"Error decoding JSON: {product_info_json}")
                name = ""
                product_id = ""
                price_per_kg = 0.0
                brand = ""
                category = ""

        # Get product image URL
        image_tag = tile.find("img", class_="ct-tile-image")
        image_url = image_tag["data-src"] if image_tag else ""

        # Get price per unit (if available)
        price_per_unit_tag = tile.find("div",
                                       class_="pwc-tile--price-secondary")
        price_per_unit = price_per_unit_tag.get_text(
            strip=True) if price_per_unit_tag else None

        # Get minimum quantity information
        min_quantity_tag = tile.find("p", class_="pwc-tile--quantity")
        min_quantity = min_quantity_tag.get_text(
            strip=True) if min_quantity_tag else None

        # Get product link
        product_link_tag = tile.find("a", href=True)
        product_link = product_link_tag["href"] if product_link_tag else ""

        # Append extracted data to list
        product_data.append({
            "Product Name": name,
            "Product ID": product_id,
            "Price per kg": price_per_kg,
            "Price per unit": price_per_unit,
            "Brand": brand,
            "Category": category,
            "Image URL": image_url,
            "Minimum Quantity": min_quantity,
            "Product Link": product_link
        })

    # Create a DataFrame from the list of dictionaries
    df = pd.DataFrame(product_data)
    df["cgid"] = cgid
    return df


# Function to fetch a page of products with caching and retry behavior
# @lru_cache(maxsize=None)
@retry_on_failure(retries=3, delay=60)
def fetch_page(start, sz, cgid, pmin, srule):
    url = "https://www.continente.pt/on/demandware.store/Sites-continente-Site/default/Search-UpdateGrid"
    headers = {
        "Accept":
        "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Encoding":
        "gzip, deflate, br, zstd",
        "Accept-Language":
        "pt-PT,pt;q=0.8,en;q=0.5,en-US;q=0.3",
        "Connection":
        "keep-alive",
        "Host":
        "www.continente.pt",
        "User-Agent":
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:133.0) Gecko/20100101 Firefox/133.0"
    }
    params = {
        "cgid": cgid,
        "pmin": pmin,
        # "srule": srule,
        "start": start,
        "sz": sz,
    }
    response = requests.get(url, params=params, headers=headers)
    response.raise_for_status()  # Raise an error if the request failed

    # check this
    soup = BeautifulSoup(response.text, 'html.parser')

    # Find all product tiles
    product_tiles = soup.find_all("div", class_="product-tile")

    # Initialize lists to store product data
    product_data = []

    with open('data.html', 'w') as f:
        f.write(response.text)

    return response.text


# Main function to fetch all products for a given category
@retry_on_failure(retries=3, delay=60)
def fetch_all_products_for_category(cgid,
                                    sz=216,
                                    pmin="0.01",
                                    srule="FRESH-Peixaria"):
    products = []
    current_start = 0
    total_products = None

    while total_products is None or current_start < total_products:
        # Fetch the current page with caching and retry
        html_content = fetch_page(current_start, sz, cgid, pmin, srule)

        # Parse total products only on the first page load
        if total_products is None:
            total_products = parse_total_products(html_content)
            if total_products is None:
                print(
                    f"Failed to retrieve total products count for category {cgid}."
                )
                return pd.DataFrame(products)

        # Parse products from current page
        page_products = parse_product_data(html_content, cgid)
        products.append(page_products)

        print(
            f"Fetched {min(current_start + sz, total_products)} of {total_products} products for category {cgid}"
        )

        # Move to the next batch
        current_start += sz
        time.sleep(random.randint(5,
                                  10))  # Random delay to avoid server overload

    df = pd.concat(products)
    df["tracking_date"] = datetime.now().strftime("%Y-%m-%d")
    df["source"] = "Continente"

    return df


def process_and_save_categories():
    initial_url = "https://www.continente.pt/"
    initial_response = requests.get(initial_url)
    # base_path = "continente_price_tracker/data/"

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    CATEGORIES = [
        "congelados", "frescos", "mercearias", "bebidas", "biologicos",
        "limpeza", "higiene-beleza", "bebe"
    ]

    for category in CATEGORIES:
        print(f"Processing category: {category}")
        df_category_products = fetch_all_products_for_category(category)
        if not df_category_products.empty:
            filename = f"{category}_{timestamp}.csv"
            # filename = base_path + filename
            df_category_products.to_csv(filename, index=False)
            print(f"Saved data for category '{category}' to {filename}")
        else:
            print(f"No data found for category {category}.")


# Call the function to execute
process_and_save_categories()
