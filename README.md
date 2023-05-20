# TP1: Bike analyzer

## Usage instructions
To start the script to create the docker compose, run the following command:

```make regenerate-docker```

If you want to change the default numbers on it there are two possibilities:

- Change the number on the make file to be what you desired (default 3).
- Use the command: 
``python3 addFields.py stationsAmounts tripsAmount weatherAmount distributorsAmount calculatorAmount``
Which will recreate the docker-compose properly.

To start the processes that will listen for input, just run the following command:

``make start-app``

To see the logs:

``make logs``

To start running the script, direct to the folder client and the run:

``python3 pedro.py``
