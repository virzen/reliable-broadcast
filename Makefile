all: install run

install:
	pipenv install

run:
	mpiexec --use-hwthread-cpus pipenv run python3 main.py
