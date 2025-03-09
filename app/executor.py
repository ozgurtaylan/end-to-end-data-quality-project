import pandas as pd
import time
import logging
import config

class Executor:
    def __init__(self, controller, db, assertion_handler):
        self._controller = controller
        self._db = db
        self._assertion_handler = assertion_handler

    def execute(self) -> float:
        start_time = time.time()
        conn = None

        try:
            conn = self._db.open_connection()
            logging.info(f"Connection to {self._controller.table_name} opened successfully.")

            table_data = self._db.fetch_table_in_chunks(
                conn=conn, 
                table_name=self._controller.table_name, 
                columns=self._controller.columns
            )

            for chunk in table_data:
                for control in self._controller.controls:
                    result = control["method"](chunk, control["column_name"])
                    control["result"] += result
                    control["status"] = "FAILURE" if control["result"] > 0 else "SUCCESS"

            logging.info(f"Validation completed for {self._controller.table_name}.")
        
        except Exception as e:
            logging.error(f"Error during execution for {self._controller.table_name}: {e}", exc_info=True)

        finally:
            if conn:
                self._db.close_connection(conn=conn)
                logging.info(f"Connection to {self._controller.table_name} closed.")

        end_time = time.time()
        elapsed_time = round(end_time - start_time, 2)
        logging.info(f"Execution time: {elapsed_time} seconds.")
        return elapsed_time

    def report_stats(self):
        for control in self._controller.controls:
            urn = f"urn:li:assertion:{self._controller.table_name}-{control['control_name']}"
            entity_urn = config.DATAHUB_ENTITY_URN
            assertion_type = f"CONTROL-{self._controller.table_name}"
            description = control["control_name"]
            field_path = control["column_name"]

            try:
                upsert_success = self._assertion_handler.upsert_assertion(
                    urn=urn,
                    entity_urn=entity_urn,
                    assertion_type=assertion_type,
                    description=description,
                    field_path=field_path,
                )

                if upsert_success:
                    logging.info(f"Upserted assertion successfully: {urn}")
                else:
                    logging.warning(f"Failed to upsert assertion: {urn}")

                # Sleep to avoid overwhelming the server
                time.sleep(0.5)

                result_type = control["status"]
                properties = {"key": "count", "value": str(control["result"])}

                report_success = self._assertion_handler.report_assertion_result(
                    urn=urn,
                    result_type=result_type,
                    properties=properties
                )

                if report_success:
                    logging.info(f"Reported assertion result successfully: {urn} with status {result_type}")
                else:
                    logging.warning(f"Failed to report assertion result: {urn}")

                # Sleep to avoid overwhelming the server
                time.sleep(0.5)

            except Exception as e:
                logging.error(f"Unexpected error during assertion reporting for {urn}: {e}", exc_info=True)
