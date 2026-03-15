import logging

def logger_config():
    # Configure log format
    formato = '%(asctime)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=formato,
                        handlers=[
                            logging.FileHandler("registro.log"),
                            logging.StreamHandler()
                        ])



logger_config()
logger = logging.getLogger(__name__)
#logger.basicConfig(level=logging.INFO)
