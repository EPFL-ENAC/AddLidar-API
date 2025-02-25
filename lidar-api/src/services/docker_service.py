from src.config.settings import settings


def run_lidar_cli(command_args: list[str]) -> tuple[bytes, int]:
    import subprocess

    try:
        result = subprocess.run(command_args, check=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        if result.returncode == 0:
            return result.stdout, 0
        # TODO: remove when addlidarmanager is fixed
        if result.returncode == 1:
            return result.stdout, 0
        return result.stderr, result.returncode
    except Exception as e:
        return str(e).encode('utf-8'), 1

def process_point_cloud(file_path: str, cli_args: list[str]) -> tuple[bytes, int]:
    command_args = ["docker", "run", "-v", f"{file_path}:/data", settings.DOCKER_IMAGE]
    command_args.extend(cli_args)
    return run_lidar_cli(command_args)
