from src.config.settings import settings


def run_lidar_cli(command_args: list[str]) -> tuple[bytes, int]:
    import subprocess

    try:
        result = subprocess.run(
            command_args, check=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )
        if result.returncode == 0:
            return result.stdout, 0
        # TODO: remove when addlidarmanager is fixed
        if result.returncode == 1:
            return result.stdout, 0
        return result.stderr, result.returncode
    except Exception as e:
        return str(e).encode("utf-8"), 1


def process_point_cloud(file_path: str, cli_args: list[str]) -> tuple[bytes, int]:
    command_args = [f"{file_path}:/data"]
    command_args.extend(cli_args)
    import docker

    client = docker.from_env()
    client.login(
        username=settings.GH_USERNAME,
        password=settings.GH_PAT,
        registry=settings.REGISTRY,
    )
    client.images.pull(settings.IMAGE_NAME, tag=settings.IMAGE_TAG)
    output = client.containers.run(
        settings.IMAGE_NAME,
        command_args,
        volumes={
            f"{settings.ROOT_VOLUME}": {"bind": "/data", "mode": "ro"},
        },
        network="enac-cd-app_default",
    )
    output = output.decode("utf-8")
    print(output, flush=True)
    return run_lidar_cli(command_args)
