from pathlib import Path

import pytest
import yaml

import src.common.constants as constants
from src.services.ConfigReader import ConfigReader


@pytest.fixture
def temp_config_file(tmp_path, monkeypatch):
    """
    Creates a temporary YAML config file and monkeypatches CONFIG_PATH
    and JOB_CONFIG_FILE_PATH so ConfigReader picks it up.
    """
    # Create fake config dict
    config_data = {
        "job": {
            "input": {
                "path": "/data/input/sales.csv",
                "format": "csv",
                "options": {"header": "true", "inferSchema": "true"},
            }
        },
        "sparkConf": {"spark.app.name": "TestApp", "spark.master": "local[*]"},
        "output": {"path": "/data/output", "mode": "overwrite"},
    }

    # Write YAML file
    config_file = tmp_path / "job_config.yaml"
    with open(config_file, "w") as f:
        yaml.dump(config_data, f)

    # Monkeypatch constants to point to our temp file
    monkeypatch.setattr(constants, "CONFIG_PATH", "")
    monkeypatch.setattr(constants, "JOB_CONFIG_FILE_PATH", config_file.name)

    return config_file


def test_get_config_file_path(temp_config_file):
    reader = ConfigReader(temp_config_file)
    assert Path(reader.get_config_file_path()).name == temp_config_file.name


def test_get_config(temp_config_file):
    reader = ConfigReader(temp_config_file)
    config = reader.get_config()
    assert "job" in config.keys()
    assert "sparkConf" in config
    assert "output" in config


def test_get_key_existing(temp_config_file):
    reader = ConfigReader(temp_config_file)
    assert reader.get_key("output") == {"path": "/data/output", "mode": "overwrite"}


def test_get_key_default(temp_config_file):
    reader = ConfigReader(temp_config_file)
    assert reader.get_key("nonexistent", default="missing") == "missing"


def test_get_job_config(temp_config_file):
    reader = ConfigReader(temp_config_file)
    job_config = reader.get_job_config()
    assert "input" in job_config


def test_get_data_file_path(temp_config_file):
    reader = ConfigReader(temp_config_file)
    assert reader.get_data_file_path() == "/data/input/sales.csv"


def test_get_spark_config(temp_config_file):
    reader = ConfigReader(temp_config_file)
    spark_conf = reader.get_spark_config()
    assert spark_conf["spark.app.name"] == "TestApp"


def test_get_input_config(temp_config_file):
    reader = ConfigReader(temp_config_file)
    input_conf = reader.get_input_config()
    assert input_conf["format"] == "csv"


def test_get_format_config(temp_config_file):
    reader = ConfigReader(temp_config_file)
    assert reader.get_format_config() == "csv"


def test_get_options_config(temp_config_file):
    reader = ConfigReader(temp_config_file)
    options = reader.get_options_config()
    assert options["header"] == "true"
    assert options["format"] == "csv"  # injected by get_options_config


def test_get_output_config(temp_config_file):
    reader = ConfigReader(temp_config_file)
    output = reader.get_output_config()
    assert output["path"] == "/data/output"
