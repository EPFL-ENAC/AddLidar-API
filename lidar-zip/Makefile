build:
	docker build -t lidar-zip .


run:
	docker run --rm \
		-v ./lidar:/lidar:ro \
		-v ./zips:/zips \
		lidar-zip:latest /lidar/0003_EPFL/01_Lidar /zips/0003_EPFL/01_Lidar.tar.gz


debug:
	docker run --rm -it \
		-v ./lidar:/lidar:ro \
		-v ./zips:/zips \
		--entrypoint /bin/bash \
		lidar-zip:latest