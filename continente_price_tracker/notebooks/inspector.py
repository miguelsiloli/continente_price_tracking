import pandas as pd
import re
from glob import glob
import os

data2 = pd.read_csv("data/auchan/limpeza-da-casa-e-roupa_20241112_111011.csv")
data = data2["product_name"]

# Define regex patterns for weight, volume, length, and item quantities
weight_pattern = r'(\d+(\.\d+)?)\s*(kg|g|l|ml)'  # Matches weight/volume like 0.75l, 355g
unit_pattern = r'(\d+)\s*(un|pieces|dozen|unit|meia\s*dúzia|par|rolos|doses)'  # Matches quantities like 4 units, 12 pieces, etc.
complex_weight_pattern = r'(\d+)\s*x\s*(\d+(\.\d+)?)\s*(kg|g|l|ml)'  # Matches patterns like 4x200g
volume_pattern = r'(\d+)\s*x\s*(\d+)\s*(cl|ml)'  # Matches volume-based quantities (e.g., 6x25cl)
length_pattern = r'(\d+)\s*(mt|m)'  # Matches length-based quantities (e.g., 20mt)
dose_pattern = r'(\d+)\s*(doses)'  # Matches doses (e.g., 60 doses)
pair_pattern = r'(\d+)\s*(par)'  # Matches pairs (e.g., 1par)

def parse_quantity(product_name):
    """
    Parse the product quantity, weight, unit, and quantity from the product description.
    
    Parameters:
    - product_name (str): The product description or name.
    
    Returns:
    - tuple: (weight_value, weight_unit, quantity) where:
        - weight_value is the numeric value of weight/volume (float)
        - weight_unit is the unit of measurement (str)
        - quantity is the quantity of units (int or float)
    """
    weight_value = None
    weight_unit = None
    quantity = None
    
    # Check for weight or volume (e.g., 0.75l, 355g)
    weight_match = re.search(weight_pattern, product_name)
    if weight_match:
        weight_value = float(weight_match.group(1))  # Numeric value
        weight_unit = weight_match.group(3)  # Unit (kg, g, l, ml)
    
    # Check for complex weight (e.g., 4x200g)
    complex_weight_match = re.search(complex_weight_pattern, product_name)
    if complex_weight_match:
        quantity = int(complex_weight_match.group(1))  # Extract the quantity of units (e.g., 4)
        weight_value = float(complex_weight_match.group(2))  # Extract the weight per unit (e.g., 200g)
        weight_unit = complex_weight_match.group(4)  # Extract the unit (e.g., g, kg, etc.)

    # Check for volume-based quantities (e.g., 6x25cl)
    volume_match = re.search(volume_pattern, product_name)
    if volume_match:
        quantity = int(volume_match.group(1))  # Extract the quantity of units (e.g., 6)
        weight_value = float(volume_match.group(2))  # Extract the volume per unit (e.g., 25)
        weight_unit = volume_match.group(3)  # Extract the unit (cl, ml)
        
    # Correct handling for volume vs. length distinction (i.e., m vs. ml)
    if 'm' in product_name and 'ml' not in product_name:
        length_match = re.search(length_pattern, product_name)
        if length_match:
            quantity = int(length_match.group(1))  # Extract quantity of length (e.g., 20)
            weight_value = None  # No weight/volume value here
            weight_unit = length_match.group(2)  # Unit (m, mt)
    
    # Check for doses (e.g., 60 doses)
    dose_match = re.search(dose_pattern, product_name)
    if dose_match:
        quantity = int(dose_match.group(1))  # Extract quantity of doses (e.g., 60)
        weight_value = None  # No weight/volume value here
        weight_unit = 'doses'  # The unit is "doses"

    # Check for pairs (e.g., 1par)
    pair_match = re.search(pair_pattern, product_name)
    if pair_match:
        quantity = int(pair_match.group(1))  # Extract quantity of pairs (e.g., 1)
        weight_value = None  # No weight/volume value here
        weight_unit = 'par'  # The unit is "pair"

    # Check for unit-based quantity (e.g., 4 units, 12 pieces, etc.)
    unit_match = re.search(unit_pattern, product_name)
    if unit_match:
        quantity = int(unit_match.group(1))  # Extract quantity for units (e.g., 4 units)
        weight_value = None  # No weight provided
        weight_unit = unit_match.group(2)  # Extract the unit like 'un', 'pieces', 'dozen'
        
        # If the unit is 'dozen' or 'meia dúzia', convert to quantity of items
        if weight_unit in ['dozen', 'meia dúzia']:
            weight_value = quantity * 12  # Convert dozen to items (e.g., 12 eggs)

    # If no weight was found and there's no quantity, return None
    if weight_value is None and quantity is None:
        return None, None, None
    
    return weight_value, weight_unit, quantity

# Conversion functions for weight and volume units
def convert_to_grams(weight_value, weight_unit):
    """
    Convert the weight to grams.

    Parameters:
    - weight_value (float): The weight value to be converted.
    - weight_unit (str): The weight unit (kg, g, l, ml, etc.).

    Returns:
    - float: The converted weight value in grams (g) or milliliters (ml).
    """
    if weight_unit == 'kg':
        return weight_value * 1000  # Convert kg to grams
    elif weight_unit == 'g':
        return weight_value  # No conversion needed for grams
    elif weight_unit == 'l':
        return weight_value * 1000  # Convert liters to milliliters
    elif weight_unit == 'ml':
        return weight_value  # No conversion needed for milliliters
    else:
        return None  # If the unit is not recognized

def convert_to_ml(weight_value, weight_unit):
    """
    Convert the volume to milliliters (ml).

    Parameters:
    - weight_value (float): The volume value to be converted.
    - weight_unit (str): The volume unit (l, ml, etc.).

    Returns:
    - float: The converted volume value in milliliters (ml).
    """
    if weight_unit == 'l':
        return weight_value * 1000  # Convert liters to milliliters
    elif weight_unit == 'ml':
        return weight_value  # No conversion needed for milliliters
    else:
        return None  # If the unit is not recognized

def convert_quantities_to_standard(product_data):
  """
  Converts quantities and units into a standardized form of grams (g) or milliliters (ml).
  
  This function converts the given weight and volume units into a standard form (grams and milliliters).
  It handles kilograms, grams, liters, milliliters, and special units like dozens, pairs, and doses.
  
  Parameters:
  - product_data (tuple): A tuple containing (weight_value, weight_unit, quantity).
  
  Returns:
  - tuple: (standardized_weight_value, standardized_weight_unit, standardized_quantity)
  """
  weight_value, weight_unit, quantity = product_data
  
  # Convert weight/volume units to standard grams/ml
  if weight_unit in ['kg', 'g', 'l', 'ml']:
      standardized_weight_value = convert_to_grams(weight_value, weight_unit)
      standardized_weight_unit = 'g' if weight_unit in ['kg', 'g'] else 'ml'  # For weight, we use grams, for volume we use milliliters
  else:
      standardized_weight_value = None  # No conversion needed for non-weight/volume units
      standardized_weight_unit = None
  
  # Handle quantities for non-weight-related units
  if weight_unit in ['unit', 'un', 'par', 'pieces', 'dozen', 'meia dúzia', 'rolos', 'doses']:
      standardized_quantity = quantity
  else:
      standardized_quantity = None
  
  return standardized_weight_value, standardized_weight_unit, standardized_quantity
   
def process_and_calculate_price(path_to_files):
    # Get list of all CSV files in the specified directory
    csv_files = glob(os.path.join(path_to_files, "*.csv"))
    
    # List to hold all DataFrames
    all_data_frames = []
    
    for file in csv_files:
        print(f"Processing file: {file}")
        
        # Load the data (assumes each file has a column named 'product_name' and 'product_description')
        data = pd.read_csv(file)

        # Parse the quantities and convert them
        parsed_data = [parse_quantity(product) for product in data['product_name']]
        parsed_data = [convert_quantities_to_standard(product) for product in parsed_data]
        
        # Convert the parsed data into a DataFrame
        df = pd.DataFrame(parsed_data, columns=['Weight_Value', 'Weight_Unit', 'Quantity'])

        # Join with the original data to add product details like name and price
        df2 = df.join(data[["product_name", "product_price"]])
        
        # Calculate the price per unit
        df2["price_per_unit"] = df2["product_price"] / df2["Weight_Value"]
        
        # Append the current DataFrame to the list
        all_data_frames.append(df2)

    # Concatenate all DataFrames into one unique DataFrame
    final_df = pd.concat(all_data_frames, ignore_index=True)
    
    # Define the output file path
    output_file = os.path.join(path_to_files, "processed_all_products.csv")
    
    # Save the resulting combined DataFrame to a unique CSV file
    final_df.to_csv(output_file, index=False)
    print(f"Saved combined processed data to {output_file}")

# Define the path to the directory containing the CSV files
path_to_auchan_data = "data/auchan"

# Call the function to process and calculate prices
process_and_calculate_price(path_to_auchan_data)