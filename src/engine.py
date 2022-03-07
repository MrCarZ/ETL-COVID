from .transform import Transform

class Engine:
    def __init__(self, api_path):
        self.pipeline = self.run_etl(api_path)
    
    def run_etl(self, api_path):
        if(api_path == ''):
            raise Exception("API Path is invalid")
        else:
            return Transform(api_path)

    