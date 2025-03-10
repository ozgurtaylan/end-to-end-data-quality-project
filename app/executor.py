import time
import logging
from database import Database
from controllers.controller_factory import ControllerFactory
from assertion_handler import AssertionHandler

logger = logging.getLogger("quality_cheks")

class Executor:
    def __init__(self, config):
        self._config = config
        self._controllers = self._init_controllers()

    def _init_controllers(self):
        self._controllers = []
        controller_configs = self._config["controllers"]
        for config in controller_configs:
            controller = ControllerFactory.create_controller(config)
            self._controllers.append(controller)
        return self._controllers

    def execute_controllers(self) -> float:
        start_time = time.time()
        for controller in self._controllers:
            conn = None
            try:
                db = Database(
                            db_type=controller.db_type, 
                            db_user=controller.db_user, 
                            db_password=controller.db_password, 
                            db_host=controller.db_host, 
                            db_port=controller.db_port, 
                            db_name=controller.db_name, 
                            conn_idle_timeout=controller.conn_idle_timeout)
                conn = db.open_connection()
                logger.info(f"Connection to {controller.table_name} opened successfully.")
                table_data = db.fetch_table_in_chunks(conn=conn, chunksize=controller.chunk_size, table_name=controller.table_name)
                for chunk in table_data:
                    for control in controller.executor_feed:
                        result, status = getattr(controller, control["method"])(chunk, control["column"])
                        control["result"] += result
                        control["status"] = status
                logger.info(f"Validation completed for {controller.table_name}.")
            except Exception as e:
                logger.error(f"Error during execution for {controller.table_name}: {e}", exc_info=True)
            finally:
                if conn:
                    db.close_connection(conn)
                    logger.info(f"Connection to {controller.table_name} closed.")
        end_time = time.time()
        elapsed_time = round(end_time - start_time, 2)
        logger.info(f"Execution time: {elapsed_time} seconds.")
        return elapsed_time

    # Open to improvements...
    def upsert_assertions(self): 
        for controller in self._controllers:
            assertion_handler = AssertionHandler(controller.datahub_server_url, controller.datahub_platform_urn)
            try: 
                for control in controller.executor_feed:
                    urn = f"urn:li:assertion:{controller.table_name}-{control['control_name']}"
                    entity_urn = controller.datahub_entity_urn
                    assertion_type = f"quality-checks-{controller.table_name}"
                    description = control["control_name"]
                    field_path = control["column"]
                    upsert_success = assertion_handler.upsert_assertion(
                        urn=urn,
                        entity_urn=entity_urn,
                        assertion_type=assertion_type,
                        description=description,
                        field_path=field_path,
                    )
                    if upsert_success:
                        logger.info(f"Upserted assertion successfully: {urn}")
                    else:
                        logger.warning(f"Failed to upsert assertion: {urn}")
                        time.sleep(0.1)

                time.sleep(0.1)
            except Exception as e:
                logger.error(f"Unexpected error during upserting assertions: {e}", exc_info=True)
            finally:
                assertion_handler.close()

    # Open to improvements...
    def report_assertion_results(self): 
        for controller in self._controllers:
            assertion_handler = AssertionHandler(controller.datahub_server_url, controller.datahub_platform_urn)
            try: 
                for control in controller.executor_feed:
                    urn = f"urn:li:assertion:{controller.table_name}-{control['control_name']}"
                    result_type = control["status"]
                    properties = {"key": "count", "value": str(control["result"])}
                    report_success = assertion_handler.report_assertion_result(
                        urn=urn,
                        result_type=result_type,
                        properties=properties
                    )
                    if report_success:
                        logger.info(f"Reported assertion result successfully: {urn} with status {result_type}")
                    else:
                        logger.warning(f"Failed to report assertion result: {urn}")
                    time.sleep(0.1)

                time.sleep(0.1)
            except Exception as e:
                logger.error(f"Unexpected error during reporting: {e}", exc_info=True)
            finally:
                assertion_handler.close()
