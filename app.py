import requests
from db.db_connection import DbConnection

URL = "https://minhareceita.org/"

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

create_transaction_data()



