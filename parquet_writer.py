import datetime
import os
import pathlib

import log

logger = log.setup_custom_logger(__name__)


class ParquetWriter:

    def save_feed_df(self, output_root, df):
        path = self.__generate_output_path(output_root)
        logger.info("Saving feed df to: " + path)
        self.save_df(path, df)

    # TODO: string interpolation?
    @staticmethod
    def __generate_output_path(output_root):
        today = datetime.datetime.utcnow().date()
        time = datetime.datetime.utcnow().time()
        path = output_root + str(today) + "/" + str(time) + ".parquet"
        return path


    @staticmethod
    def save_df(path, df):
        if df.empty:
            return

        pathlib.Path(os.path.dirname(path)).mkdir(parents=True, exist_ok=True)

        logger.info("Saved df\n" + str(df.count()))
        df.to_parquet(path)
