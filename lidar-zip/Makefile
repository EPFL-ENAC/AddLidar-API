build:
	docker build -t lidar-zip .

run:
	docker run --rm \
		-v ./lidar:/lidar:ro \
		-v ./zips:/zips \
		lidar-zip:latest /lidar /zips

debug:
	docker run --rm -it \
		-v ./lidar:/lidar:ro \
		-v ./zips:/zips \
		--entrypoint /bin/bash \
		lidar-zip:latest