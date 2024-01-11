# access token
# sl.Bh6YlThdaNNBXg673uNMIYYHHuQ4rKYEShdzjCsLQslS39cVRdxm9e0sD20fpxljDbTIZ_FfQoy-kSfytfz3OWMRRLySu0vTx6ECA69_mtuPrGjPvYxzMRi4gl3Fv2K8Mn0AioOH# YCNFubrAC9GNRpbIoHwJudqE94oGwb0IbKPvCuL3
import dropbox

from clickhouse_driver import Client

import os
from prefect import flow, task
import os
from prefect import flow, task
from prefect.task_runners import SequentialTaskRunner
from prefect_aws import AwsCredentials
from prefect_aws.s3 import S3Bucket
import pandas as pd
import re

from gensim.models import LdaModel
from nltk.corpus import stopwords

from nltk.stem import WordNetLemmatizer
wordnet_lemmatizer= WordNetLemmatizer()
from gensim import corpora

from pathlib import Path

access_token = "sl.Bh5GJMDnkZDZhuUDcGisRJGSs3a3rTgzzBQOIJSL6Nz8LJ0KJ5IFaJ3LLEcHQVZhp9Z-uc8TBae-lfQNW_wdH_siyfCPzUIVnibeDpAQBiZ8knVrMdwjJtYVfcUdPCMp2wxp_XMv"

@task(retries=2,retry_delay_seconds=5 )
def get_dropbox_context(access_token):
    dbx = dropbox.Dropbox(access_token)
    return dbx

@task(retries=2,retry_delay_seconds=10 )
def extract_file(context) -> Path:
    dropbox_file = "/news_nyt.csv"
    local_file = os.path.dirname(__file__) + '/nyt_news.csv'
    
    with open(local_file, "wb") as f:
        metadata, res = context.files_download(path=dropbox_file)
        f.write(res.content)
    
    return local_file


@task()
def transform(file_path) -> pd.DataFrame:
    
    stoplist= stopwords.words('english')
    stoplist.extend(['mrs','dr','ms','woman','man','news','company'])

    df = pd.read_csv(file_path)
    df.drop(columns=['id', 'date_counter'], inplace=True)
    df.rename(columns={"Unnamed: 0": "Id",
              "front_page": "Print_version"}, inplace=True)
    for c in df.columns:
        df.rename(columns={c: c.capitalize()}, inplace=True)
    df['Stems'] = df['Stems'].apply(lambda x: x.lower())
    df['Stems'] = df['Stems'].apply(lambda x: re.sub(r"(@\[A-Za-z0-9]+)|([^0-9A-Za-z \t])|(\w+:\/\/\S+)|^rt|http.+?", "", x))
    df['Stems'] = df['Stems'].apply(lambda x: " ".join([word for word in x.split() if word not in (stoplist)]))
    texts = df['Stems'].apply(lambda x: x.split(' ')).tolist()
    
    dictionary = corpora.Dictionary(texts)

    dictionary.filter_extremes(no_below=2)

    dictionary.compactify()

    corpus = [dictionary.doc2bow(text) for text in texts]
    topic_model = LdaModel(
        corpus, id2word=dictionary, num_topics=20, iterations=200)
    
    topic_dict = {i: str([token for token, score in topic_model.show_topic(i, topn=2)]) for i in range(0, topic_model.num_topics)}

    df['Topic'] = df['Stems'].apply(lambda x: topic_dict[max(
        topic_model[topic_model.id2word.doc2bow(x.split())], key=lambda x:x[1])[0]])
    
    return df

@task()
def create_parquet(df):
    local_parquet_path = os.path.dirname(__file__) + '/nyt_news.parquet'
    df.to_parquet(local_parquet_path, index=False, compression="gzip")
    
    return local_parquet_path
    
@task(task_run_name="load-{format}-file-to-s3")
def load_to_s3(from_path, format):
    AwsCredentials.load("yandex-cloud-s3-credential")
    s3_bucket = S3Bucket.load("yandex-cloud-s3-bucket")
    s3_bucket.upload_from_path(from_path=from_path)


    
@flow(log_prints=True,task_runner=SequentialTaskRunner)
def etl_news():
    dbx = get_dropbox_context.submit(access_token=access_token)
    csv_file_path = extract_file.submit(dbx)
    df = transform.submit(csv_file_path)
    parquet_file_path = create_parquet.submit(df)
    load_to_s3.submit(from_path=csv_file_path, format = 'csv')
    load_to_s3.submit(from_path = parquet_file_path, format='parquet')
    # etl_news_to_ch()

if __name__ == "__main__":
    etl_news()