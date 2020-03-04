# Costpuller

This pulls in cost data from the cost management system, performs a range of consistency checks on it and outputs it in the format used for the cluster cost reporting.

Call the binary with `--help` for commandline options.

Accounts are specified in the file `accounts.yaml`. Run the binary in the same directory of this file. The standard value and max deviation is checked against the total pulled from cost management. Reports are written to a seperate file and console. Deviation is not checked when standard value is given as 0.

Call this with one single commandline parameter containing the cookie used for accessing the cost management in CURL format. Use your Chrome browser to copy the cookie in CURL format.
