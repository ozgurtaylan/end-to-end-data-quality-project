from typing import Dict, Type
import logging
from .base_controller import BaseController
from .product_controller import ProductController

logger = logging.getLogger(__name__)

class ControllerFactory:

    __controllers: Dict[str, Type] = {
        "ProductController": ProductController,
    }

    @classmethod
    def create_controller(cls, controller_config: Dict) -> BaseController:
        if "class_name" not in controller_config:
            logger.error("Controller configuration missing 'class_name'.")
            raise KeyError("Missing 'class_name' in controller configuration")
        class_name = controller_config.pop("class_name")
        if class_name not in cls.__controllers:
            logger.error("Controller class %s not found", class_name)
            raise ValueError(f"Controller class {class_name} not registered")
        controller_class = cls.__controllers[class_name]
        logger.info("Creating controller %s.", class_name)
        return controller_class(**controller_config)
    
    