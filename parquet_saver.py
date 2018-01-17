import datetime
import os
import pandas as pd
import log

logger = log.setup_custom_logger(__name__)


def save_feed_df(exchange, df):
    logger.info("Saving feed df")
    path = generate_path(exchange)
    q_path = generate_quarantine_path(exchange)
    save_df(path, df, q_path)


def generate_path(exchange):
    now = datetime.datetime.utcnow()
    path = "parquet/" + exchange + "/orderbook/feed/" + str(now) + ".parquet"
    return path

def generate_quarantine_path(exchange):
    q_path = "parquet/" + exchange + "/orderbook/feed/quarantine/"
    return q_path


def save_df(path, df, quarantine_path):

    if df.empty:
        return

    try:
        existing_df = pd.read_parquet(path)
        df_to_save = existing_df.append(df)
        df_to_save.drop_duplicates()
    except:
        logger.info("Could not read existing df")
        if os.path.exists(path):
            logger.info("Path exists, moving corrupt file to quarantine")
            now = datetime.datetime.utcnow()
            os.rename(path, quarantine_path + str(now) + ".parquet")

        df_to_save = df

    logger.info("Saved df\n" + str(df_to_save.count()))
    df_to_save.to_parquet(path)