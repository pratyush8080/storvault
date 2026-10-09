.PHONY: build test tsan up down bench k8s
build:
	cmake -S agent -B agent/build -G Ninja -DCMAKE_BUILD_TYPE=Release && cmake --build agent/build
test: build
	python3 -m pytest tests -q
tsan:
	cmake -S agent -B agent/build-tsan -G Ninja -DSV_TSAN=ON -DCMAKE_BUILD_TYPE=RelWithDebInfo && cmake --build agent/build-tsan && ./agent/build-tsan/sv_tests
up:
	docker compose -f deploy/docker-compose.yml up -d --build
down:
	docker compose -f deploy/docker-compose.yml down
bench: build
	scripts/bench.sh
k8s:
	scripts/k8s-up.sh
