# Installation and usage

To run the scraper you need a running MongoDB instance and the modules listed in `requirements.txt` must be installed.
Before you launch the crawler, you need to put in the same folder in which you launch the script a file whose name is `data.txt` with the following structure.
```
<MongoDB connection string>
<Telegram API ID 0>
<Telegram API Hash 0>
...
<Telegram API ID N>
<Telegram API Hash N>
```
Now you can launch the `main.py` script, you may want to run it inside a `screen` session.

# Code description

The code has a master-worker architecture in which the master can assign the following tasks to the worker.
- `TRY_JOIN`. The worker to whom this task is assigned tries to join a group identified by the group username. In case of success, the worker gathers the messages starting from a specified date.
- `CHECK_UPDATES`. The worker to whom this task is assigned gathers messages from a group identified by its unique id starting from a given offset date (the date when last update occurred).
- `CHECK_WAIT`. The worker to whom this task is assigned checks whether a join request that was previously sent to a group identified by its unique id has been approved. In case of success, the worker gathers the messages starting from a specified date.
- `CHECK_USERNAME`. The worker to whom this task is assigned checks whether the username of a group whose unique id is given has changed and updates it.

Tasks are assigned with different priorities depending on the specified mode the crawler is launched with (`crawl()` method of `MonitoringMaster`).
- `both`. Tasks `CHECK_WAIT`, `CHECK_UPDATES` and `TRY_JOIN` are assigned in this order until no new groups are available.
- `check`. Tasks `CHECK_WAIT` and `CHECK_UPDATES` are assigned in this order for a given time interval (24 hours by default).
- `join`. Task `TRY_JOIN` is assigned until no new groups are available.

The state of a group (`state` field in the `groups` collection in the database) changes during the dispatchment of a task and as the master checks the results of the assigned task.
The following picture describes how state changes depending on assigned tasks and result codes.
![Finite state machine of the groups in the crawler.](state_fsm.png)