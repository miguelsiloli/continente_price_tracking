from catalog import process_and_save_categories
from pingo_doce.pingo_doce import parse_and_save_all_categories
from auchan.auchan import get_and_parse_auchan_data

# continente
# process_and_save_categories()

# pingo doce
# Example usage
categories = [
    "pingo-doce-lacticinios", "pingo-doce-bebidas",
    "pingo-doce-frescos-embalados", "pingo-doce-higiene-e-beleza",
    "pingo-doce-maquinas-e-capsulas-de-cafe", "pingo-doce-mercearia",
    "pingo-doce-refeicoes-prontas"
]

parse_and_save_all_categories(categories)

cgid_list = [
    "alimentacao-", "biologico-e-escolhas-alimentares",
    "limpeza-da-casa-e-roupa", "bebidas-e-garrafeira", "marcas-auchan"
]

# auchan
prefn1 = "soldInStores"
prefv1 = "000"
sz = 212
base_url = "https://www.auchan.pt/on/demandware.store/Sites-AuchanPT-Site/pt_PT/Search-UpdateGrid"

# Loop through each cgid and save the corresponding DataFrame to a CSV file
for cgid in cgid_list:
  final_data = get_and_parse_auchan_data(cgid, prefn1, prefv1, sz, base_url)
  final_data.to_csv(f"{cgid}.csv", index=False)
  print(f"Data for {cgid} saved to {cgid}.csv")
