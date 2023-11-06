import json
import requests
from time import sleep, time
from brazilcep import get_address_from_cep, WebService
from geopy.geocoders import Nominatim
from db.db_connection import DbConnection
from dotenv import dotenv_values


def create_transaction_data():
    """Method responsible for extract CNPJs data and insert into database with minhareceita.org
    """
    URL_MINHA_RECEITA = "https://minhareceita.org/"

    start = time()
    query = """
        SELECT
            id_transacao,
            cod_documento_envolvido
        FROM "pfm-service".en_transacao
        WHERE tp_transacao = 'PRE_PAGO'
            AND cod_documento_envolvido IS NOT NULL
            AND tp_envolvido = 'PESSOA_JURIDICA'
            AND id_transacao not in (
                SELECT id_transacao 
                FROM "pfm-service".en_transacao_cnae
            )
    """

    db = DbConnection()
    records = db.run_query(query, get_all=True)
    print("Get data from DB")
    
    count = 0

    for record in records:
        print("Making request")
        response = requests.get(url=URL_MINHA_RECEITA+record[1])

        response_dict = response.json()

        if response.status_code == 200:
            print("Inserting data on DB")
            data_insert = db.run_query(
                f"""
                INSERT INTO "pfm-service".en_transacao_cnae 
                VALUES(
                    {record[0]}, 
                    '{record[1]}', 
                    {response_dict.get("cnae_fiscal", None)},
                    '{response_dict.get("cnae_fiscal_descricao", None).strip()}',
                    '{response_dict.get("cep", None).strip()}'
                )
                """
            )
            print(f"{count} Transaction {record[0]} inserted")
        else:
            print(f"Request failed, status code: {response.status_code}")
        count += 1
    end = time()
    print(f"Data insert done in {round(end - start, 2)}")

def create_cep_data_with_geopy():
    """Method responsible for get geolocation from brazilian address with geopy
    """
    query = """
        SELECT num_cep
        FROM "atualizacao_cadastral".en_cep_lat_long
        WHERE latitude IS NULL and longitude IS NULL
    """

    db = DbConnection()
    records = db.run_query(query, get_all=True)
    print("Get data from DB")

    geolocator = Nominatim(user_agent="test_app")
   
    count = 0

    for record in records:
        address = None
        try:
            address = get_address_from_cep(record[0], webservice=WebService.APICEP)
        except Exception as e:
            address = None

        if (
            address
            and len(address.get("street")) > 0
            and len(address.get("city")) > 0
            and len(address.get("district")) > 0
        ):
            print(f"Get address from CEP: {record[0]}")

            location = geolocator.geocode(
                address['street'] + ", " + address['city'] + " - " + address['district']
            )
            print(f"Get coordinates from CEP with complete address")

            if location and location.latitude and location.longitude:
     
                data_insert = db.run_query(
                    f"""
                    UPDATE "atualizacao_cadastral".en_cep_lat_long
                    SET latitude = '{location.latitude}', longitude = '{location.longitude}'
                    WHERE num_cep = '{record[0]}'
                    """
                )
                print(f"{count} Inserted data on DB")
            elif location and not location.latitude and not location.longitude:
                data_insert = db.run_query(
                    f"""
                    UPDATE "atualizacao_cadastral".en_cep_lat_long
                    SET latitude = '{None}', longitude = '{None}'
                    WHERE num_cep = '{record[0]}'
                    """
                )
                print(f"{count} Inserted data on DB with lat and long None")
            else:
                print(f"{count} Location not found")
            print("Wating a time to make the request again")
            sleep(3)
        elif address and len(address.get("city")) > 0:
            print(f"Get address from CEP: {record[0]}")

            location = geolocator.geocode(
                address['city']
            )
            print(f"Get coordinates from CEP with city only")

            if location and location.latitude and location.longitude:
     
                data_insert = db.run_query(
                    f"""
                    UPDATE "atualizacao_cadastral".en_cep_lat_long
                    SET latitude = '{location.latitude}', longitude = '{location.longitude}'
                    WHERE num_cep = '{record[0]}'
                    """
                )
                print(f"{count} Inserted data on DB")
            elif location and not location.latitude and not location.longitude:
                data_insert = db.run_query(
                    f"""
                    UPDATE "atualizacao_cadastral".en_cep_lat_long
                    SET latitude = '{None}', longitude = '{None}'
                    WHERE num_cep = '{record[0]}'
                    """
                )
                print(f"{count} Inserted data on DB with lat and long None")
            else:
                print(f"{count} Location not found")
            print("Wating a time to make the request again")
            sleep(3)
        else:
            print(f"{count} Address not found for CEP: {record[0]}")
        count += 1


def create_cep_data_with_cep_aberto():
    """Method responsible for get geolocation from brazilian postal codes with cep aberto
    """
    URL_CEP_ABERTO = "https://www.cepaberto.com/api/v3/cep?cep="

    query = """
        with asd as (
            select distinct num_cep
            from "atualizacao_cadastral".cadastro_sicredi cs
            union select distinct num_cep
            from "atualizacao_cadastral".cadastro_opf co
        )
        select num_cep
        from asd
        where num_cep is not null
            and num_cep <> 'NA'
            and length(num_cep) <> 1
            and num_cep not in (
                select num_cep 
                from "atualizacao_cadastral".en_cep_lat_long
            )
    """

    print("Get data from DB")
    db = DbConnection()
    records = db.run_query(query, get_all=True)
    
    header = {"Authorization": f"Token token={dotenv_values(".env")["CEP_ABERTO_API_KEY"]}"}

    count = 0
    for record in records:
        if count == 0:
            print("Requests limit reached")
            break
        try:
            response = requests.get(url=URL_CEP_ABERTO+record[0], headers=header)
        except Exception as e:
            print("Some problem hapened with response")
            response = None

        if response and response.status_code == 200:
            response_dict = response.json()
            print(f"Get coordinates from CEP: {record[0]}")

            data_insert = db.run_query(
                f"""
                INSERT INTO "atualizacao_cadastral".en_cep_lat_long 
                VALUES( 
                    '{record[0]}', 
                    '{response_dict.get("latitude", None)}',
                    '{response_dict.get("longitude", None)}'
                )
                """
            )
            print(f"{count} Inserted data on DB")
        else:
            print(f"{count} Something it wrong with the request, response status code: {response.status_code}")
        count += 1

def create_cep_data_with_bing_maps():
    """Method responsible for get geolocation from brazilian postal codes with bing maps
    """
    start_time = time()

    api_url = "http://dev.virtualearth.net/REST/v1/Locations"

    query = """
        with asd as (
            select distinct num_cep
            from "atualizacao_cadastral".cadastro_sicredi cs
            union select distinct num_cep
            from "atualizacao_cadastral".cadastro_opf co
        )
        select num_cep
        from asd
        where num_cep is not null
            and num_cep <> 'NA'
            and length(num_cep) <> 1
            and num_cep not in (
                select num_cep 
                from "atualizacao_cadastral".en_cep_lat_long_bing_maps
            )
    """
    print("Get data from DB")
    db = DbConnection()
    records = db.run_query(query, get_all=True)

    count = 0
    for record in records:
        payload = {
            "countryRegion": "brazil",
            "postalCode": f"{record[0]}",
            "key": dotenv_values(".env")["BING_API_KEY"]
        }

        response = requests.get(url=api_url, params=payload)

        if response and response.status_code == 200:
            response_dict = response.json()
            print(f"Get coordinates from CEP: {record[0]}")

            if (
                len(response_dict.get("resourceSets")) > 0 
                and len(response_dict.get("resourceSets")[0].get("resources")) > 0
                and len(response_dict.get("resourceSets")[0].get("resources")[0].get("point").get("coordinates")) > 0
            ):
                received_data = (
                    response_dict.get("resourceSets", None)[0].get("resources", None)[0].get("point", None).get("coordinates", None)
                )

                if received_data:
                    data_insert = db.run_query(
                        f"""
                        INSERT INTO "atualizacao_cadastral".en_cep_lat_long_bing_maps 
                        VALUES( 
                            '{record[0]}', 
                            '{received_data[0]}',
                            '{received_data[1]}'
                        )
                        """
                    )
                    print(f"{count} Inserted data on DB")

                with open(f"./requests_json_raw/{record[0]}.json", "w") as file:
                    file.write(json.dumps(response_dict, indent=4))
                    print(f"created a json file for CEP {record[0]}")
            else:
                print(f"{count} Something went wrong with received data")
        else:
            print(f"{count} Something it wrong with the request, response status code: {response.status_code}")
        count += 1
    end_time = time()
    print(f"All data was charged in {round(end_time - start_time, 2)} hours")

def create_cep_data_with_azure_maps():
    """Method responsible for get geolocation from brazilian postal codes with azure maps
    """
    url = "https://atlas.microsoft.com/search/fuzzy/json"

    payload = {
        "api-version": 1.0,
        "query": "cep",
        "countrySet": "BR"
        "subscription-key": dotenv_values(".env")["AZURE_API_KEY"],
    }




def main():
    command = input(
        """Choose one function: \n
        1 - Create CEP data with geopy \n
        2 - Create CEP data with CEP aberto API \n
        3 - Create transaction data with minha receita \n
        4 - Create CEP data with Bing Maps API \n
        5 - Create CEP data with Azure Maps API \n
        """
    )

    match command:
        case "1":
            create_cep_data_with_geopy()
        case "2":
            create_cep_data_with_cep_aberto()
        case "3":
            create_transaction_data()
        case "4":
            create_cep_data_with_bing_maps()
        case: "5":
            create_cep_data_with_azure_maps()
        case _:
            print("Choice unavailable")

main()

