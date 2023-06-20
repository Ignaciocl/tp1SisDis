# Tests

Each tests within this directory does the following:

+ `test_workers.sh`: stops **worker-station1**, **worker-trip1** and **worker-weather1**.

+ `test_joiners.sh`: stops **joiner-montreal**, **joiner-stations** and **joiner-weather**.

+ `test_server.sh`: stops the server.

+ `test_mix.sh`: stops the following service **worker-station1**, **joiner-stations** and **server**.

+ `test_kill_all`: stops all nodes from a given stage. E.g: `./test_kill_all worker-trip` will stop all the workers related with trips. Possible inputs are:
  + **worker-trip**
  + **worker-station**
  + **worker-weather**
  + **joiner-montreal**
  + **joiner-stations**
  + **joiner-weather**