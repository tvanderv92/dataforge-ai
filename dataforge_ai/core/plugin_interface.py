from abc import ABC, abstractmethod
from typing import Any, Dict
import logging

class PluginInterface(ABC):
    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)

    @abstractmethod
    def execute(self, input_data: Dict[str, Any]) -> Any:
        """
        Execute the main functionality of the plugin.

        :param input_data: A dictionary containing the input data for the plugin.
        :return: The result of the plugin execution.
        """
        pass

    @abstractmethod
    def validate_input(self, input_data: Dict[str, Any]) -> bool:
        """
        Validate the input data for the plugin.

        :param input_data: A dictionary containing the input data for the plugin.
        :return: True if the input is valid, False otherwise.
        """
        pass

    def get_metadata(self) -> Dict[str, Any]:
        """
        Get metadata about the plugin.

        :return: A dictionary containing metadata about the plugin.
        """
        return {
            "name": self.__class__.__name__,
            "description": self.__doc__,
            "input_schema": self.get_input_schema(),
            "output_schema": self.get_output_schema(),
        }

    @abstractmethod
    def get_input_schema(self) -> Dict[str, Any]:
        """
        Get the schema for the input data expected by the plugin.

        :return: A dictionary representing the input schema.
        """
        pass

    @abstractmethod
    def get_output_schema(self) -> Dict[str, Any]:
        """
        Get the schema for the output data produced by the plugin.

        :return: A dictionary representing the output schema.
        """
        pass

    def log_execution(self, message: str, level: str = "info"):
        """
        Log a message related to the plugin execution.

        :param message: The message to log.
        :param level: The log level (default is "info").
        """
        log_method = getattr(self.logger, level.lower())
        log_method(f"[{self.__class__.__name__}] {message}")
