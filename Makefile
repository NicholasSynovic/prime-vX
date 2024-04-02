build:
	poetry build
	pip install --no-deps dist/*.tar.gz
