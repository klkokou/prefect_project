from clickhouse_driver import Client

import pandas as pd
import os


from prefect import flow, task
from prefect_aws import AwsCredentials
from prefect_aws.s3 import S3Bucket
    
@task()
def fetch() -> pd.DataFrame:
    AwsCredentials.load("yandex-cloud-s3-credential")

    s3_path = "nyt_news.parquet"
    local_path= os.path.dirname(__file__) + '/nyt_news.parquet'
    s3_block = S3Bucket.load("yandex-cloud-s3-bucket")
    s3_block.download_object_to_path(from_path=s3_path, to_path=local_path)
    df = pd.read_parquet(local_path)
    return df

def create_table(table: str) -> str:
    return f'''
    CREATE TABLE IF NOT EXISTS default.{table}
    (
        Id                Int64,
        Title             String,
        Author            String,
        Content           String,
        Date              String,
        Year              Int64,
        Month             Int64,
        Print_version     Bool,
        Url               String,
        Stems             String,
        Topic             String 
    )
    ENGINE = MergeTree()
    ORDER BY Id
    PRIMARY KEY Id;
    '''


def drop_table(table: str) -> str:
    return f'DROP TABLE IF EXISTS default.{table};'


@task()
def upload_ch(df: pd.DataFrame, table: str) -> None:

    conn_congif = {'host': 'rc1a-gai8gpmq9ekifu6k.mdb.yandexcloud.net',
                "user":'admin',
                "password":'564U8FRPeuHfTxh',
                "port":9440,
                "secure":True,
                "verify":True,
                "ca_certs":'/home/dora/prefect_projects/summer_practice_2023/YandexInternalRootCA.crt'}
    con = Client(**conn_congif)

    print("Connection:", con.connection)

    sql_query = drop_table(table)
    con.execute(sql_query)
    sql_query = create_table(table)
    con.execute(sql_query)
    con.insert_dataframe(f'INSERT INTO default.{table} ( Id, Title, Author, Content, Date, Year,Month, Print_version, Url, Stems, Topic) VALUES',df, settings={'use_numpy': True})
    
   
@flow(log_prints=True)
def etl_news_to_ch():
    upload_ch(fetch(), table="nyt_news")

if __name__ == "__main__":
    etl_news_to_ch()
    