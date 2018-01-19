import datetime
import os
import pathlib

import log

logger = log.setup_custom_logger(__name__)


def save_feed_df(exchange, df):
    logger.info("Saving feed df for "+exchange)
    path = generate_feed_path(exchange)
    save_df(path, df)


def save_trades_df(exchange, df):
    logger.info("Saving trades df for " + exchange)
    path = generate_trades_path(exchange)

    save_df(path, df)


# TODO: string interpolation?
def generate_feed_path(exchange):
    today = datetime.datetime.utcnow().date()
    time = datetime.datetime.utcnow().time()
    path = "parquet/" + exchange + "/orderbook/feed/" + str(today) + "/" + str(time) + ".parquet"
    return path


def generate_trades_path(exchange):
    today = datetime.datetime.utcnow().date()
    time = datetime.datetime.utcnow().time()
    path = "parquet/" + exchange + "/orderbook/trades/" + str(today) + "/" + str(time) + ".parquet"
    return path


def save_df(path, df):

    if df.empty:
        return

    pathlib.Path(os.path.dirname(path)).mkdir(parents=True, exist_ok=True)

    logger.info("Saved df\n" + str(df.count()))
    df.to_parquet(path)