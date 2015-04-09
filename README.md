# cf-mysql-quota-enforcer
Quota enforcer for cf-mysql-release

## Usage

### Configuration

The quota enforcer executable requires a config file. The default location for this file is assumed to be `./config.yml`, and can be overridden using `-configFile=/path/to/config.yml` flag.

 An example configuration file is provided in `config-example.yml`. Copy this to
`config.yml` and edit as necessary; `config.yml` is ignored by git.

##Testing

Unit tests can be run by executing

```sh
./bin/test-unit
```

Integration tests can be run by executing

```sh
./bin/test-integration
```

Configuration for the integration tests is managed by environment variables; see
`./bin/test-integration` for further details.
