import yaml


class ConfigReader:

    def __init__(self, file_path):
        self.__file_path = file_path
        self._config_date = self.__load_config()

    def get_config_file_path(self):
        return self.__file_path

    def __load_config(self):
        with open(self.__file_path) as file:
            return yaml.safe_load(file)

    def get_config(self):
        return self._config_date

    def get_key(self, key, default=None):
        if isinstance(self._config_date, dict) and key in self._config_date:
            return self._config_date[key]
        return default

    def get_job_config(self):
        return self.get_key("job")

    def get_data_file_path(self):
        return self.get_job_config().get("input").get("path")

    def get_spark_config(self):
        return self.get_key("sparkConf")

    def get_input_config(self):
        return self.get_job_config().get("input")

    def get_format_config(self):
        return self.get_job_config().get("input").get("format")

    def get_options_config(self):
        options = self.get_input_config().get("options")
        options["format"] = self.get_format_config()
        return options

    def get_output_config(self):
        return self.get_key("output")
