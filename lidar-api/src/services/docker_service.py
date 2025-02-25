def run_lidar_cli(command_args: list[str]) -> str:
    import subprocess

    try:
        result = subprocess.run(command_args, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        return result.stdout.decode('utf-8')
    except subprocess.CalledProcessError as e:
        return e.stderr.decode('utf-8')


def process_point_cloud(file_path: str, cli_args: list[str]) -> str:
    command_args = ["docker", "run", "-v", f"{file_path}:/data", "lidardatamanager"]
    command_args.extend(cli_args)
    return run_lidar_cli(command_args)
