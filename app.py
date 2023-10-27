import requests
from time import sleep
from brazilcep import get_address_from_cep, WebService
from geopy.geocoders import Nominatim
from db.db_connection import DbConnection

URL = "https://minhareceita.org/"
URL_CEP = "https://www.cepaberto.com/api/v3/cep?cep="

def create_transaction_data():
    """Method responsible for extract data from api and insert into database"""
    query = """
        SELECT
            id_transacao,
            cod_documento_envolvido
        FROM "pfm-service".en_transacao
        WHERE tp_transacao = 'PRE_PAGO'
            AND cod_documento_envolvido IS NOT NULL
            AND tp_envolvido = 'PESSOA_JURIDICA'
    """

    db = DbConnection()
    records = db.run_query(query, get_all=True)
    print("Get data from DB")
    
    count = 0

    for record in records:
        count += 1
        
        print("Making request")
        response = requests.get(url=URL+record[1])

        response_dict = response.json()

        if response.status_code == 200:
            print("Inserting data on DB")
            data_insert = db.run_query(
                f"""
                INSERT INTO "pfm-service".en_transacao_cnae 
                VALUES(
                    {record[0]}, 
                    {record[1]}, 
                    {response_dict.get("cnae_fiscal", None)},
                    '{response_dict.get("cnae_fiscal_descricao", None).strip()}'
                )
                """
            )
            print(f"{count} Transaction {record[0]} inserted")
        else:
            print("Request failed")

    print("Data insert done")


def create_cep_data_with_geopy():
    """Method responsible for create latitude and longitude to brazilian postal codes
    """
    query = """
        SELECT num_cep, num_cpf
        FROM "atualizacao_cadastral".en_cep_geolocalizacao eng
        WHERE eng.latitude = 'None' and eng.longitude = 'None'
    """

    db = DbConnection()
    records = db.run_query(query, get_all=True)
    print("Get data from DB")

    geolocator = Nominatim(user_agent="test_app")
   
    records.reverse()
    count = 0

    for record in records[1815:]:
        count += 1
        if count >= 0:
            address = None
            try:
                address = get_address_from_cep(record[0], webservice=WebService.APICEP)
            except Exception as e:
                address = None

            if address:
                print(f"Get address from CEP: {record[0]}")

                location = geolocator.geocode(
                    address['street'] + ", " + address['city'] + " - " + address['district']
                )
                print(f"Get coordinates from CEP")

                if location and location.latitude and location.longitude:
         
                    data_insert = db.run_query(
                        f"""
                        UPDATE "atualizacao_cadastral".en_cep_geolocalizacao
                        SET latitude = '{location.latitude}', longitude = '{location.longitude}'
                        WHERE num_cep = '{record[0]}'
                        """
                    )
                elif location and not location.latitude and not location.longitude:
                    data_insert = db.run_query(
                        f"""
                        UPDATE "atualizacao_cadastral".en_cep_geolocalizacao
                        SET latitude = '{None}', longitude = '{None}'
                        WHERE num_cep = '{record[0]}'
                        """
                    )
                print(f"{count} Inserted data on DB")
                print("Wating a time to make the request again")
                sleep(5)
            else:
                print(f"{count} Address not found for CEP: {record[0]}")
        else:
            print(f"{count} Skiping data inserted")


def create_cep_data_with_cep_aberto():
    query = """
        SELECT num_cep, num_cpf
        FROM "atualizacao_cadastral".en_cep_aux_2
    """

    print("Get data from DB")
    db = DbConnection()
    records = db.run_query(query, get_all=True)
    
    header = {"Authorization": "Token token=ed77a865e243242c3dd1a51a2a047c7a"}

    count = 1476
    for record in records:
        count -= 1
        if count == 0:
            print("Requests limit reached")
            break
        try:
            response = requests.get(url=URL_CEP+record[0], headers=header)
        except Exception as e:
            print("Some problem hapened with response")
            response = None

        if response and response.status_code == 200:
            response_dict = response.json()
            print(f"Get coordinates from CEP: {record[0]}")

            data_insert = db.run_query(
                f"""
                INSERT INTO "atualizacao_cadastral".en_cep_geolocalizacao 
                VALUES(
                    '{record[1]}', 
                    '{record[0]}', 
                    '{response_dict.get("latitude", None)}',
                    '{response_dict.get("longitude", None)}'
                )
                """
            )
            print(f"{count} Inserted data on DB")
        else:
            print(f"{count} Something it wrong with the request, response status code: {response.status_code}")



#create_cep_data_with_geopy()
create_cep_data_with_cep_aberto()
