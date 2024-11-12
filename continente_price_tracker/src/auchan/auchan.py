import requests
from bs4 import BeautifulSoup
from tqdm import tqdm
import os
import pandas as pd
import time
import json
from datetime import datetime
from utils import retry_on_failure


def parse_products_from_html(html_content):
    """
    Parses HTML content to extract product information and returns it as a DataFrame.

    Args:
        html_content (str): The raw HTML content of the page to be parsed.

    Returns:
        pd.DataFrame: A DataFrame containing parsed product information, such as product ID, name, price, categories, image, and other attributes.
    """
    soup = BeautifulSoup(html_content, 'html.parser')

    # Define the schema for the product information DataFrame
    product_schema = {
        "product_id": pd.Series(dtype='str'),
        "product_name": pd.Series(dtype='str'),
        "product_price": pd.Series(dtype='float'),
        "product_category": pd.Series(dtype='str'),
        "product_category2": pd.Series(dtype='str'),
        "product_category3": pd.Series(dtype='str'),
        "product_image": pd.Series(dtype='str'),
        "product_urls": pd.Series(dtype='str'),
        "product_ratings": pd.Series(dtype='str'),
        "product_labels": pd.Series(dtype='str'),
        "product_promotions": pd.Series(dtype='str'),
        "quantity_selector": pd.Series(dtype='str')
    }

    # Create an empty DataFrame with the defined schema
    product_df = pd.DataFrame(product_schema)

    # Find all product elements in the HTML
    products = soup.find_all('div', class_='product')

    product_list = []

    for product in products:
        product_data = {}

        # Extract product ID
        product_data['product_id'] = product['data-pid']

        # Extract product URLs
        product_urls = product.find('div', class_='product-tile')['data-urls']
        product_data['product_urls'] = product_urls

        # Extract product name
        product_name = product.find('div',
                                    class_='pdp-link').find('a').text.strip()
        product_data['product_name'] = product_name

        # Extract product price
        product_price = product.find('span', class_='value')['content']
        product_data['product_price'] = float(product_price)

        # Extract product category
        product_category = product.find('div',
                                        class_='product-tile')['data-gtm-new']
        product_data['product_category'] = product_category

        # Extract nested product categories
        product_category_data = json.loads(product_category)
        product_data['product_category'] = product_category_data.get(
            'item_category', None)
        product_data['product_category2'] = product_category_data.get(
            'item_category2', None)
        product_data['product_category3'] = product_category_data.get(
            'item_category3', None)

        # Extract product image URL
        product_image = product.find(
            'div', class_='image-container').find('img')['src']
        product_data['product_image'] = product_image

        # Extract product ratings
        product_ratings = product.find(
            'div', class_='auc-product-tile__bazaarvoice--ratings'
        )['data-bv-product-id']
        product_data['product_ratings'] = product_ratings

        # Extract product labels
        product_labels = []
        labels = product.find_all('img', class_='auc-product-labels__icon')
        for label in labels:
            product_labels.append({
                'alt': label['alt'],
                'title': label['title'],
                # 'src': label['src']
            })
        product_data['product_labels'] = product_labels

        # Extract product promotions (assign None if not found)
        product_promotions = product.find('div',
                                          class_='auc-price__promotion__label')
        product_data['product_promotions'] = product_promotions.text.strip(
        ) if product_promotions else None

        # Extract quantity selector details (assign None if not found)
        quantity_selector = product.find('div', class_='auc-qty-selector')
        if quantity_selector:
            product_data['quantity_selector'] = quantity_selector
        else:
            product_data['quantity_selector'] = None

        # Convert nested dictionaries to JSON strings for DataFrame compatibility
        product_data["product_urls"] = str(product_data["product_urls"])
        product_data["product_ratings"] = str(product_data["product_ratings"])
        product_data["product_labels"] = str(product_data["product_labels"])
        product_data["quantity_selector"] = str(
            product_data["quantity_selector"])

        # Append the product data to the list
        product_list.append(product_data)

    # Convert the list of product data to a DataFrame
    product_df = pd.concat([product_df, pd.DataFrame(product_list)],
                           ignore_index=True)

    # Reindex the DataFrame to ensure it has the same columns as the schema
    product_df = product_df.reindex(columns=product_schema.keys())

    # Assert that the DataFrame structure is consistent with the schema
    assert list(product_df.columns) == list(
        product_schema.keys()), "DataFrame structure does not match the schema"

    return product_df


@retry_on_failure(retries=3, delay=60)
def get_auchan_data(cgid, prefn1, prefv1, start, sz, next, selectedUrl):
    """
    Fetches HTML data from the Auchan store's search API endpoint.

    Args:
        cgid (str): The category group ID used for filtering the products.
        prefn1 (str): The name of the first preference filter.
        prefv1 (str): The value of the first preference filter.
        start (int): The starting index for the product list.
        sz (int): The number of products to fetch per request.
        next (str): Whether or not to fetch the next page of products (usually "true").
        selectedUrl (str): The URL that includes the parameters for the request.

    Returns:
        str: The raw HTML content from the Auchan store's search results.
    """
    url = "https://www.auchan.pt/on/demandware.store/Sites-AuchanPT-Site/pt_PT/Search-UpdateGrid"
    headers = {
        "User-Agent":
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.75 Safari/537.36",
        "Accept":
        "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "TE": "Trailers"
    }
    params = {
        "cgid": cgid,
        "prefn1": prefn1,
        "prefv1": prefv1,
        "start": start,
        "sz": sz,
        "next": next
    }

    response = requests.get(url, headers=headers, params=params)

    if response.status_code == 200:
        return response.text
    else:
        response.raise_for_status()


@retry_on_failure(retries=3, delay=60)
def get_and_parse_auchan_data(cgid, prefn1, prefv1, sz, base_url):
    """
    Retrieves and parses product data from the Auchan store in a paginated manner.

    Args:
        cgid (str): The category group ID used for filtering the products.
        prefn1 (str): The name of the first preference filter.
        prefv1 (str): The value of the first preference filter.
        sz (int): The number of products to fetch per request.
        base_url (str): The base URL of the search results.

    Returns:
        pd.DataFrame: A DataFrame containing parsed product information across multiple pages.
    """
    start = 0
    all_data = pd.DataFrame()

    # idk about the total value
    with tqdm(total=20, unit='batch') as pbar:
        while True:
            selectedUrl = f"{base_url}?cgid={cgid}&prefn1={prefn1}&prefv1={prefv1}&start={start}&sz={sz}&next=true"

            try:
                data = get_auchan_data(cgid, prefn1, prefv1, start, sz, "true",
                                       selectedUrl)
                parsed_data = parse_products_from_html(data)

            except:
                # im assuming if this breaks its probably because data is empty of products
                return all_data

            all_data = pd.concat([all_data, parsed_data], ignore_index=True)

            pbar.update(1)
            time.sleep(3)

            if len(parsed_data) < sz:
                break

            start += sz
            pbar.total += 1  # Increase the total count dynamically

    return all_data


def save_data_for_all_cgids(cgid_list,
                            prefn1,
                            prefv1,
                            sz,
                            base_url,
                            base_path="data"):
    """
    Fetches and processes product data for each cgid in the list and saves it to CSV files in the specified directory.

    Args:
        cgid_list (list): List of cgid values for which data will be fetched.
        prefn1 (str): The first filter parameter for fetching data.
        prefv1 (str): The value corresponding to the prefn1 filter.
        sz (int): The number of items to fetch per request.
        base_url (str): The base URL for fetching data.
        base_path (str): The directory where the CSV files will be saved. Defaults to "data".
    """
    # Ensure the base path exists; if not, create it
    if not os.path.exists(base_path):
        os.makedirs(base_path)
        print(f"Directory '{base_path}' created.")

    # Create a timestamp for unique filenames
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    # Loop through each cgid and fetch & save the corresponding data
    for cgid in cgid_list:
        print(f"Processing cgid: {cgid}")

        # Fetch and parse the data for the given cgid
        final_data = get_and_parse_auchan_data(cgid, prefn1, prefv1, sz,
                                               base_url)

        if not final_data.empty:
            # Create a filename with timestamp and cgid
            filename = f"{cgid}_{timestamp}.csv"
            file_path = os.path.join(base_path,
                                     filename)  # Full path with base directory

            # Save the data to CSV
            final_data.to_csv(file_path, index=False)
            print(f"Data for {cgid} saved to {file_path}")
        else:
            print(f"No data found for {cgid}. Skipping...")


# List of cgid values
cgid_list = [
    "alimentacao-", "biologico-e-escolhas-alimentares",
    "limpeza-da-casa-e-roupa", "bebidas-e-garrafeira", "marcas-auchan"
]

if __name__ == "__main__":
    # Example Usage
    cgid_list = ['category1', 'category2',
                 'category3']  # Replace with actual cgid list
    prefn1 = "soldInStores"
    prefv1 = "000"
    sz = 212
    base_url = "https://www.auchan.pt/on/demandware.store/Sites-AuchanPT-Site/pt_PT/Search-UpdateGrid"

    save_data_for_all_cgids(cgid_list,
                            prefn1,
                            prefv1,
                            sz,
                            base_url,
                            base_path="data/auchan")
