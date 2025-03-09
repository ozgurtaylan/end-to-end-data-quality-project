import time
import logging
from typing import Dict, Literal
from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph
import config

class AssertionHandler:
    def __init__(self, server_url=config.DATAHUB_SERVER_URL, platform_urn=config.DATAHUB_PLATFORM_URN):
        self._server_url = server_url
        self._platform_urn = platform_urn
        self._graph = self._initialize_datahub_client()
        logging.info(f"AssertionHandler initialized for {self._platform_urn}")

    def _initialize_datahub_client(self) -> DataHubGraph:
        try:
            client_config = DatahubClientConfig(server=self._server_url)
            logging.info(f"Initializing DataHubGraph with config: {client_config}")
            return DataHubGraph(config=client_config)
        except Exception as e:
            logging.error(f"Failed to initialize DataHubGraph: {e}")

    def upsert_assertion(self, urn: str, entity_urn: str, assertion_type: str, description: str, field_path: str) -> bool:
        try:
            self._graph.upsert_custom_assertion(
                urn=urn,
                entity_urn=entity_urn,
                type=assertion_type,
                description=description,
                platform_urn=self._platform_urn,
                field_path=field_path
            )
            logging.info(f"Successfully upserted assertion: {urn}")
            return True
        except Exception as e:
            logging.error(f"Error during upserting assertion: {e}")
            return False

    def report_assertion_result(self, urn: str, result_type: Literal["SUCCESS", "FAILURE", "ERROR", "INIT"], properties: Dict[str, str]) -> bool:
        try:
            self._graph.report_assertion_result(
                urn=urn,
                timestamp_millis=int(time.time() * 1000),
                type=result_type,
                properties=[{"key": key, "value": value} for key, value in properties.items()],
            )
            logging.info(f"Reported assertion result: {urn} - {result_type}")
            return True
        except Exception as e:
            logging.error(f"Error during assertion report: {e}")
            return False
        
    def close(self):
        try:
            self._graph.close()
            logging.info("AssertionHandler client closed successfully.")
        except Exception as e:
            logging.error(f"Error closing the DataHubGraph client: {e}")
