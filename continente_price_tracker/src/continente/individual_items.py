from bs4 import BeautifulSoup

def parse_nutritional_info(html_content):
    soup = BeautifulSoup(html_content, "html.parser")
    
    # Parse reference intake
    reference_intake = soup.find('div', class_='daily-value-intake-reference')
    reference_text = reference_intake.find_all('p')[1].get_text(strip=True) if reference_intake else "Not found"
    
    # Parse serving size
    serving_size = soup.find('div', class_='serving-size')
    serving_size_value = serving_size.find_all('p')[1].get_text(strip=True) if serving_size else "Not found"
    
    # Parse unit of measure
    serving_size_uom = soup.find('div', class_='serving-size--uom')
    uom_value = serving_size_uom.find_all('p')[1].get_text(strip=True) if serving_size_uom else "Not found"
    
    # Parse nutrients table
    nutrients = []
    nutrients_table = soup.find('div', class_='nutrients-table')
    if nutrients_table:
        rows = nutrients_table.find_all('div', class_='nutrients-row')[1:]  # Skip header row
        for row in rows:
            cells = row.find_all('div', class_='nutriInfo-details')
            if len(cells) == 3:
                nutrient_name = cells[0].get_text(strip=True)
                nutrient_quantity = cells[1].get_text(strip=True)
                nutrient_unit = cells[2].get_text(strip=True)
                nutrients.append({
                    "Nutrient": nutrient_name,
                    "Quantity": nutrient_quantity,
                    "Unit": nutrient_unit
                })
    
    # Parse product description
    description = {}
    description_content = soup.find('div', class_='ct-pdp--description-content')
    if description_content:
        for section in description_content.find_all('p', class_='mb-0'):
            header = section.get_text(strip=True).replace(":", "")
            content = section.find_next_sibling('p').get_text(strip=True)
            description[header] = content
    
    # Return as structured data
    return {
        "Reference Intake": reference_text,
        "Serving Size": serving_size_value,
        "Unit of Measure": uom_value,
        "Nutrients": nutrients,
        "Description": description
    }
