import logging
import yaml
import time
from executor import Executor

logging.basicConfig(
    format="%(asctime)s - [QUALITY_CHEKS] - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.StreamHandler()],
    level=logging.INFO
)
logger = logging.getLogger("QUALITY_CHEKS")

class App:

    # Open to improvements...
    CONTROLLER_CONFIG_PATH = "./app/controllers/config/config.yaml"

    def __init__(self):
        controller_configs = self.__load_config(self.CONTROLLER_CONFIG_PATH)
        self.executor = Executor(controller_configs)  
        
    def __load_config(self,path):
        try:
            with open(path, "r") as file:
                config_data = yaml.safe_load(file)
                logger.info("Loaded controller config.")
                return config_data
        except Exception as e:
            raise RuntimeError(f"Error loading controller config: {e}")

    def run(self):
        try:
            self.executor.execute_controllers()
            self.executor.upsert_assertions()
            # Wait for some time to let the assertions upserted
            # Open to improvements...
            time.sleep(5)
            self.executor.report_assertion_results()
        except Exception as e:
            logger.error(f"Error running app: {e}")

if __name__ == "__main__":
    app = App()
    app.run()
