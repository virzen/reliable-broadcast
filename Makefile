all: install run

install:
	pipenv install

run:
	mpiexec -n 4 pipenv run python3 main.py
