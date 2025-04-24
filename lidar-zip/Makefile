build:
	docker build -t lidar-zip .


run:
	docker run --rm \
		-v ./lidar:/lidar:ro \
		-v ./zips:/zips \
		lidar-zip:latest /lidar/0003_EPFL/01_Lidar /zips/0003_EPFL/01_Lidar.tar.gz


scan:
	# ./scan.sh ./lidar/0003_EPFL/01_Lidar ./zips/0003_EPFL/01_Lidar.tar.gz
	uv run python lidar-zip/scan_and_enqueue.py --original-root /mnt/lidar_data --zip-root /mnt/archives --db-path /app/state/lidar_state.db

debug:
	docker run --rm -it \
		-v ./lidar:/lidar:ro \
		-v ./zips:/zips \
		--entrypoint /bin/bash \
		lidar-zip:latest