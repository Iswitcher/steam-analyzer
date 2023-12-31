import logging


class Log:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)
        handler = logging.StreamHandler()
        handler.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s | %(levelname)s | %(message)s')
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)
    
    def info(self, text):
        self.logger.info(text)
        
    def warning(self, text):
        self.logger.warning(text)
        
    def critical(self, text):
        self.logger.critical(text)
