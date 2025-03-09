import logging
import config
from assertion_handler import AssertionHandler
from database import Database
from executor import Executor
from controllers.controller_factory import ControllerFactory
import yaml

class App:
    def __init__(self):
        self._database = Database()
        self._assertion_handler = AssertionHandler(server_url=config.DATAHUB_SERVER_URL,platform_urn=config.DATAHUB_PLATFORM_URN,)
        self._controller_configs = self.__load_config(config.CONTROLLER_CONFIG_PATH)
        self._executors = self._initialize_executors()
        
    def __load_config(self,config_path):
        try:
            with open(config_path, "r") as file:
                config_data = yaml.safe_load(file)
                logging.info("Loaded controller config.")
                return config_data.get("controllers", [])
        except Exception as e:
            raise RuntimeError(f"Error loading controller config: {e}")
    
    def _initialize_executors(self):
        executors = []
        for config in self._controller_configs:
            try:
                controller = ControllerFactory.create_controller(**config)
                executor = Executor(db=self._database, assertion_handler=self._assertion_handler, controller=controller)
                executors.append(executor)
                logging.info(f"Initialized controller: {config['name']}")
            except Exception as e:
                logging.error(f"Error initializing controller '{config['name']}': {e}", exc_info=True)
        return executors

    def run(self):
        try:
            for executor in self._executors:
                logging.info(f"Running executor for controller: {executor._controller.table_name}")
                execution_time = executor.execute()
                logging.info(f"Execution completed in {execution_time} seconds for {executor._controller.table_name}")
                executor.report_stats()
        finally:
            self._cleanup()

    def _cleanup(self):
        self._assertion_handler.close()
        logging.info("Cleanup complete. Resources released.")


if __name__ == "__main__":
    app = App()
    app.run()
