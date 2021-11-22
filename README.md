## jenkins-usage-stats generator

`jenkins-usage-stats` handles importing daily Jenkins usage reports into a database, and generating monthly reports from that data. It replaces https://github.com/jenkins-infra/infra-statistics, and is run on Jenkins project infrastructure.

### Running

#### Database

You will need to have a URL for your Postgres database, like `postgres://postgres@localhost/jenkins_usage_stats?sslmode=disable&timezone=UTC`. This will be used when running both `jenkins-usage-stats import` and `jenkins-usage-stats report`.

#### Import

Run `jenkins-usage-stats import --database "(database URL from above)" --directory (location containing daily report gzip files from usage.jenkins.io)`. Any gzip report file which hasn't already been imported will be read, line by line, into JSON, filtered for reports which should be excluded due to non-standard or SNAPSHOT Jenkins versions, not having any jobs defined, and some other filtering criteria.

Each report will then be added to the database specified. If there is already a report present in the database for the year/month, and its report time is earlier than the new report, the new report will overwrite the previous report, incrementing the monthly count. If the new report is earlier than the existing report, the existing report's monthly count is incremented but no other changes are made - we only care about the _last_ report of the month for each instance ID. 

#### Report

Run `jenkins-usage-stats report --database "(database URL from above)" --directory (output directory to write the generated reports to)`. The various reports used on https://stats.jenkins.io will be written to that output directory in the same layout as is used on the `gh-pages` branch of this repo, and its predecessor, https://github.com/jenkins-infra/infra-statistics. Data will be considered for every month _before_ the current one, so that we don't include incomplete data for this month.

### Development

#### Setup

You will need to have `make`, `go` (1.17 or later), and, if you want to run the unit tests, `docker`, installed.

#### Testing

Make sure you have Docker running, and run `make test` to execute the unit tests.

#### Format and linting

Run `make fmt lint` to format the Go code and report on any linting/static analysis problems.

### Building

Run `make build` to generate `build/jenkins-usage-stats`, compiled for your current platform.


