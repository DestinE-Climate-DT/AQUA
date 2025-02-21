import pytest
import subprocess
import yaml
import os

@pytest.mark.aqua
def test_run_aqua_analysis():

    yaml_data = {
        'job': {
            'run_checker': True
        },
        'diagnostics': {
            'run': ["dummy"],
            'dummy': {
                'script_path': "../src/aqua_diagnostics/dummy/cli_dummy.py"
            }
        }
    }

    config_file_path = "config.aqua-analysis-test.yaml"
    with open(config_file_path, 'w') as file:
        yaml.dump(yaml_data, file)

    cmd = [
        "python", "cli/aqua-analysis/aqua-analysis.py",
        "-c", "ci", "-m", "IFS", "-e", "test-tco79", "-s", "teleconnections",
        "-d", "./output", "-l", "debug", "-f", config_file_path,
    ]

    try:
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
    except subprocess.CalledProcessError as e:
        print(f"Command failed with return code: {e.returncode}")
        print(f"Stdout:\n {e.stdout}")
        print(f"Stderr:\n {e.stderr}")
        assert False, f"Command failed (see output above)"

    print("Stdout:\n", result.stdout)
    print("Stderr:\n", result.stderr)
    assert " Diagnostic dummy completed successfully" in result.stderr, "Expected output not found in stderr"

    assert os.path.exists("./output/ci/IFS/test-tco79/experiment.yaml"), "experiment.yaml not found"
    assert os.path.exists("./output/ci/IFS/test-tco79/dummy.log"), "dummy.log not found"
    assert os.path.exists("./output/ci/IFS/test-tco79/setup_checker.log"), "experiment.yaml not found"

    os.remove(config_file_path)
