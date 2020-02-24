all: install run-all

install:
	pipenv install

run-all:
	mpiexec --use-hwthread-cpus pipenv run python3 main.py

run-4:
	mpiexec -n 4 pipenv run python3 main.py
