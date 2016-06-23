.PHONY: build run


build:
	docker build -t ${USER}/ecs-rollover:local .

