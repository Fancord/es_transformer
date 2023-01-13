
test:
	docker-compose up --build --abort-on-container-exit --timeout 60 --exit-code-from test-app
