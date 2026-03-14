import requests
import csv
import time
import os

# Configurações do Raspador TJMG
BASE_URL = "https://bd.tjmg.jus.br/server/api"
COLLECTION_UUID = "910e3664-1f94-4f35-99d8-2d0747ec4ddc"
PAGE_SIZE = 40  # Configurado para ser mais eficiente na execução completa
OUTPUT_FILE = "TJMG - raspador teste 13 de maio de 2026.csv"

def get_metadata_value(metadata, key):
    """Extrai o valor de um campo de metadados se ele existir."""
    if key in metadata and len(metadata[key]) > 0:
        return metadata[key][0].get('value', '')
    return ''

def get_pdf_link(item_uuid):
    """Busca o link direto do PDF no bundle 'ORIGINAL'."""
    try:
        bundles_url = f"{BASE_URL}/core/items/{item_uuid}/bundles"
        response = requests.get(bundles_url, timeout=10)
        response.raise_for_status()
        bundles_data = response.json()
        
        bundles = bundles_data.get('_embedded', {}).get('bundles', [])
        for bundle in bundles:
            if bundle.get('name') == 'ORIGINAL':
                bitstreams_url = bundle.get('_links', {}).get('bitstreams', {}).get('href')
                if bitstreams_url:
                    b_resp = requests.get(bitstreams_url, timeout=10)
                    b_resp.raise_for_status()
                    bitstreams_data = b_resp.json()
                    bitstreams = bitstreams_data.get('_embedded', {}).get('bitstreams', [])
                    if bitstreams:
                        return bitstreams[0].get('_links', {}).get('content', {}).get('href')
    except Exception:
        pass
    return ''

def save_to_csv(data, mode='w'):
    """Salva os dados no arquivo CSV."""
    keys = ['uuid', 'titulo', 'data_emissao', 'resumo', 'link_portal', 'link_pdf']
    with open(OUTPUT_FILE, mode, newline='', encoding='utf-8') as f:
        dict_writer = csv.DictWriter(f, fieldnames=keys)
        if mode == 'w':
            dict_writer.writeheader()
        dict_writer.writerows(data)

def main():
    page = 0
    total_pages = 1
    total_elements = 0
    
    print(f"Iniciando raspagem da coleção NATJUS-MG: {COLLECTION_UUID}")
    
    while page < total_pages:
        current_batch = []
        print(f"Processando página {page}...")
        
        search_url = f"{BASE_URL}/discover/search/objects"
        params = {
            "scope": COLLECTION_UUID,
            "page": page,
            "size": PAGE_SIZE,
            "sort": "dc.date.accessioned,DESC"
        }
        
        try:
            response = requests.get(search_url, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()
            
            search_result = data.get('_embedded', {}).get('searchResult', {})
            
            if page == 0:
                total_pages = search_result.get('page', {}).get('totalPages', 1)
                total_elements = search_result.get('page', {}).get('totalElements', 0)
                print(f"Total de itens encontrados: {total_elements}")

            objects = search_result.get('_embedded', {}).get('objects', [])
            if not objects:
                break

            for obj in objects:
                item = obj.get('_embedded', {}).get('indexableObject', {})
                uuid = item.get('uuid')
                name = item.get('name')
                metadata = item.get('metadata', {})
                
                date = get_metadata_value(metadata, 'dc.date.issued')
                abstract = get_metadata_value(metadata, 'dc.description.abstract')
                uri = get_metadata_value(metadata, 'dc.identifier.uri')
                
                pdf_link = get_pdf_link(uuid)
                
                current_batch.append({
                    'uuid': uuid,
                    'titulo': name,
                    'data_emissao': date,
                    'resumo': abstract,
                    'link_portal': uri,
                    'link_pdf': pdf_link
                })
                time.sleep(0.05)

            mode = 'w' if page == 0 else 'a'
            save_to_csv(current_batch, mode=mode)
            print(f"  > Página {page} concluída.")
            page += 1
            
        except Exception as e:
            print(f"Erro na página {page}: {e}. Tentando novamente em 5s...")
            time.sleep(5)
            continue

    print(f"Raspagem finalizada! Arquivo salvo como: {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
