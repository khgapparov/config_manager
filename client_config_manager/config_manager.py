import os
import json
import requests
from urllib.parse import urlparse

class ClientConfig:
    """
    Represents a single client's configuration.
    """
    def __init__(self, name: str, endpoint_url: str, hostname: str, namespace: str = None, custom_properties: dict = None):
        if not name:
            raise ValueError("Client name cannot be empty.")
        if not endpoint_url:
            raise ValueError("Endpoint URL cannot be empty.")
        if not hostname:
            raise ValueError("Hostname cannot be empty.")

        self.name = name
        self.endpoint_url = endpoint_url
        self.hostname = hostname
        self.namespace = namespace
        self.custom_properties = custom_properties if custom_properties is not None else {}

    def to_dict(self):
        """Converts the ClientConfig object to a dictionary."""
        return {
            "name": self.name,
            "endpoint_url": self.endpoint_url,
            "hostname": self.hostname,
            "namespace": self.namespace,
            "custom_properties": self.custom_properties
        }

    @classmethod
    def from_dict(cls, data: dict):
        """Creates a ClientConfig object from a dictionary."""
        return cls(
            name=data["name"],
            endpoint_url=data["endpoint_url"],
            hostname=data["hostname"],
            namespace=data.get("namespace"),
            custom_properties=data.get("custom_properties")
        )

    def __repr__(self):
        return (f"ClientConfig(name='{self.name}', endpoint_url='{self.endpoint_url}', "
                f"hostname='{self.hostname}', namespace='{self.namespace}', "
                f"custom_properties={self.custom_properties})")

class ConfigManager:
    """
    Manages client configurations, supporting loading from URL or local file,
    and programmatic registration/updating.
    """
    def __init__(self, config_source: str = None, default_filename: str = "client_configs.json"):
        self.config_source = config_source
        self.default_filename = default_filename
        self._client_configs = {}
        self._loaded_from_source = False

        if self.config_source:
            self._load_configurations()

    def _load_configurations(self):
        """Internal method to load configurations from the specified source."""
        parsed_url = urlparse(self.config_source)

        if parsed_url.scheme in ('http', 'https'):
            self._download_and_load(self.config_source)
        elif os.path.exists(self.config_source):
            self._load_from_local_file(self.config_source)
        else:
            raise ValueError(f"Invalid config_source: '{self.config_source}'. Must be a valid URL or local file path.")
        self._loaded_from_source = True

    def _download_and_load(self, url: str):
        """Downloads configuration from a URL and loads it."""
        try:
            response = requests.get(url)
            response.raise_for_status()  # Raise an exception for HTTP errors
            config_data = response.json()
            self._parse_config_data(config_data)
            print(f"Configurations downloaded and loaded from URL: {url}")
        except requests.exceptions.RequestException as e:
            raise IOError(f"Failed to download configurations from {url}: {e}")
        except json.JSONDecodeError as e:
            raise ValueError(f"Failed to parse JSON from {url}: {e}")

    def _load_from_local_file(self, file_path: str):
        """Loads configuration from a local file."""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                config_data = json.load(f)
            self._parse_config_data(config_data)
            print(f"Configurations loaded from local file: {file_path}")
        except FileNotFoundError:
            raise FileNotFoundError(f"Configuration file not found at: {file_path}")
        except json.JSONDecodeError as e:
            raise ValueError(f"Failed to parse JSON from {file_path}: {e}")

    def _parse_config_data(self, config_data: dict):
        """Parses the loaded configuration data and populates _client_configs."""
        if not isinstance(config_data, dict):
            raise ValueError("Configuration data must be a dictionary.")

        for client_name, client_data in config_data.items():
            try:
                # Ensure 'name' field in dict matches the key
                if "name" not in client_data or client_data["name"] != client_name:
                    client_data["name"] = client_name
                    print(f"Warning: 'name' field in client config for '{client_name}' did not match the key. Corrected.")
                self._client_configs[client_name] = ClientConfig.from_dict(client_data)
            except (KeyError, ValueError) as e:
                print(f"Error parsing configuration for client '{client_name}': {e}. Skipping this client.")

    def get_client_config(self, client_name: str) -> ClientConfig:
        """
        Retrieves a specific client configuration by name.
        Raises KeyError if the client is not found.
        """
        if client_name not in self._client_configs:
            raise KeyError(f"Client configuration for '{client_name}' not found.")
        return self._client_configs[client_name]

    def register_client_config(self, client_config: ClientConfig):
        """
        Registers or updates a client configuration programmatically.
        This change is in-memory only until save_configurations() is called.
        """
        self._client_configs[client_config.name] = client_config
        print(f"Client configuration for '{client_config.name}' registered/updated in memory.")

    def save_configurations(self, file_path: str = None):
        """
        Saves the current in-memory configurations to a local JSON file.
        If file_path is None, it saves to the default_filename in the current directory.
        """
        if file_path is None:
            file_path = self.default_filename

        output_data = {name: config.to_dict() for name, config in self._client_configs.items()}
        try:
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(output_data, f, indent=4)
            print(f"Configurations saved to: {file_path}")
        except IOError as e:
            raise IOError(f"Failed to save configurations to {file_path}: {e}")

    def list_client_names(self):
        """Returns a list of all configured client names."""
        return list(self._client_configs.keys())

    def __len__(self):
        return len(self._client_configs)

    def __iter__(self):
        return iter(self._client_configs.values())

    def __contains__(self, client_name: str):
        return client_name in self._client_configs

# Example JSON configuration structure expected
# {
#     "client_a": {
#         "name": "client_a",
#         "endpoint_url": "https://api.example.com/client_a",
#         "hostname": "api.example.com",
#         "namespace": "prod",
#         "custom_properties": {
#             "api_key": "some_key_a",
#             "timeout": 30
#         }
#     },
#     "client_b": {
#         "name": "client_b",
#         "endpoint_url": "http://localhost:8080/client_b",
#         "hostname": "localhost",
#         "custom_properties": {}
#     }
# }