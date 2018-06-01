import datetime
import pathlib
from configparser import ConfigParser


class Config:

    def __init__(self, config: ConfigParser):

        self.ws_save_interval = datetime.timedelta(seconds=int(config['behaviour']['ws_save_interval']))
        self.ob_save_interval = datetime.timedelta(seconds=int(config['behaviour']['ob_save_interval']))

        self.pid_path = config['paths']['pidfile_path']

        for root in config['roots']:
            pathlib.Path(root).mkdir(parents=True, exist_ok=True)

        self.ws_save_root = config['roots']['ws_save_root']
        self.ob_save_root = config['roots']['ob_save_root']
        self.log_root = config['roots']['log_root']
