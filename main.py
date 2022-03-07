import os
from src.engine import Engine
from dotenv import load_dotenv

load_dotenv()

if __name__ == '__main__':
    API_PATH = os.getenv('API_PATH')
    main_obj = Engine(API_PATH)
    print(main_obj)
