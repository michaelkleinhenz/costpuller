# Costpuller

This pulls in cost data from the cost management system, performs a range of consistency checks on it and outputs it in the format used for the cluster cost reporting.

Accounts are specified in the file `accounts.yaml`. Run the binary in the same directory of this file.

Call this with one single commandline paramater containing the cookie used for accessing the cost management in CURL format. Use your Chrome browser to copy the cookie in CURL format.
