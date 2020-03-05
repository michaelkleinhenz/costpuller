# Costpuller

This pulls in cost data from the cost management system, performs a range of consistency checks on it and outputs it in the format used for the cluster cost reporting.

Call the binary with `--help` for commandline options.

Accounts are specified in the file `accounts.yaml`. Run the binary in the same directory of this file. The standard value and max deviation is checked against the total pulled from cost management. Reports are written to a seperate file and console. Deviation is not checked when standard value is given as 0.

Call this with one single commandline parameter containing the cookie used for accessing the cost management in CURL format. Use your Chrome browser to copy the cookie in CURL format.

## Authorizing the Client

For this to work, the client needs an authorization for the cost management system. It is gathered from a valid cookie for cost management. You can provide the cookie in CURL format (eg. copied from a browser instance where you already logged in) using the `--cookie=<cookie>` parameter or by accessing the Chrome cookie database directly (`--readcookie`). The latter only works on Chrome browsers that don't encrypt the cookie database (eg. Linux). You can give the path to the cookie database file using `--cookiedb=<path>`, otherwise the default Linux/Chrome path is used.

## Incremental Consistency Check

The client contains a simple consistency check that can be used to check if the data from cost management is consistent with data from AWS. To use this mode, provide the parameters `--consistency` and an AWS account id you want to cross-check with `--accountid=<accountid>`. For this to work, you need AWS access credentials in your environment that can access billing information on the specified account:

```
$ export AWS_ACCESS_KEY_ID=YOUR_AKID
$ export AWS_SECRET_ACCESS_KEY=YOUR_SECRET_KEY
```

This operation checks the *blended* cost. Quoting the AWS documentation:

> For billing purposes, AWS treats all accounts in an organization as if they're one account. The pricing tiers and capacity reservations of the accounts in the organization are combined into one consolidated bill, which can lower the effective price per hour for some services.

> This effective price per hour is displayed as the "blended" rate in your Cost and Usage Report or AWS Cost Explorer. The blended rate shown in the console is for informational purposes only. 

Unblended cost is the cost where prepaid resources or discounts of the payer account is applied linear. This may mean that some resources are "free" (covered by prepaid resources on the payer account) while other resources are billed the full list price. The blended costs applies the discounts and other prepaid resources on all consumption in the same rate, calculating the median of costs. Usually, the blended rate is the rate that should be looked at. Note that for a consistent cost reporting, all reports need to be based on the same type, blended or unblended to be comparable.

