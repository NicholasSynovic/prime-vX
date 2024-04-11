build:
	poetry build
	pip install dist/*.tar.gz

pipeline:
	python prime_vx/main.py vcs-git -i ../../../scratch/coc-pyright -o test.db
	python prime_vx/main.py cloc-scc -i test.db
	python prime_vx/main.py metric-loc -i test.db
	python prime_vx/main.py metric-prod -i test.db
