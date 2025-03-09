from controllers.product_controller import ProductController
from typing import Dict, Type
import logging

class ControllerFactory:
    
    _controllers: Dict[str, Type] = {
        "products": ProductController,
    }

    @classmethod
    def register_controller(cls, name: str, controller_cls: Type):
        cls._controllers[name] = controller_cls
        logging.info(f"Controller registered: {name}")

    @classmethod
    def create_controller(cls, name: str, **kwargs):
        controller_cls = cls._controllers.get(name)
        if not controller_cls:
            raise ValueError(f"Unknown controller: {name}")
        logging.info(f"Creating controller: {name}")
        return controller_cls(**kwargs)