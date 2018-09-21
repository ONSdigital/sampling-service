# sampling-service

### Setup
####Prerequisites
* Java 8 or higher
* Gradle

#### Development Setup (macOS)
To install Scala quickly you can use Homebrew ([Brew](http://brew.sh)):
```shell
brew install scala
```

### Running the Service
To compile, build and run the application use the following command:
```shell
gradle run --args 'arg1 arg2 arg3 arg4'
```

### Test
To test all test suites use:
```shell
gradle test
```

#### Integration Tests
SBR uses a test task for integration tests (or acceptance test), more details of this and SBR's testing strategy can be found on [ONS Confluence](https://collaborate2.ons.gov.uk/confluence/display/SBR/Scala+Testing).

To run integration test run:
```shell
gradle itest
```

### Packaging
To package the project in a runnable fat-jar:
```shell
gradle shadowJar
```

### Versioning
See current application version;
```shell
gradle version
```

Bump the minor version number;
```shell
gradle bumpMinor
```

Bump the major version number;
```shell
gradle bumpMajor
```

#### Release
A final build can be tagged in Git with:
```shell
gradle gitrelease -PnewVersion=1.1
```

### Troubleshooting
See [FAQ](FAQ.md) for possible and common solutions.

### Contributing
See [CONTRIBUTING](CONTRIBUTING.md) for details.

### License
Copyright ©‎ 2018, Office for National Statistics (https://www.ons.gov.uk)

Released under MIT license, see [LICENSE](LICENSE.md) for details.
