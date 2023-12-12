# Koinos MQ Cpp

This library implements an MQ client and message handler for the Koinos Blockchain Framework.

### Project Structure

This project's structure follows the [Pitchfork](https://api.csswg.org/bikeshed/?force=1&url=https://raw.githubusercontent.com/vector-of-bool/pitchfork/develop/data/spec.bs) specification.

**`build`**: An ephemeral directory for building the project. Not checked in, but excluded via `.gitignore`.

**`include`**: Contains all public headers for Koinos MQ.

**`src`**: Contains all source code and private headers for Koinos MQ.

**`tools`**: Contains additional tooling for Koinos MQ, primarily CI scripts.

### Building

Koinos MQ's build process is configured using CMake. Additionally, all dependencies are managed through Hunter, a CMake drive package manager for C/C++. This means that all dependencies are downloaded and built during configuration rather than relying on system installed libraries.

```
mkdir build
cd build
cmake -D CMAKE_BUILD_TYPE=Release ..
cmake --build . --config Release --parallel
```

You can optionally run static analysis with Clang-Tidy during the build process. Static analysis is checked in CI and is required to pass before merging pull requests.

```
cmake -D CMAKE_BUILD_TYPE=Debug -D STATIC_ANALYSIS=ON ..
```

### Programs

#### koinos_mq_client

Koinos MQ Client connects to an AMQP endpoint and sends a message configured via the command line.

#### koinos_test_driver

Koinos Test Driver listens to 'block.broadcast.accept' and logs each message it observes.

### Formatting

Formatting of the source code is enforced by ClangFormat. If ClangFormat is installed, build targets will be automatically generated. You can review the library's code style by uploading the included `.clang-format` to https://clang-format-configurator.site/.

You can build `format.check` to check formattting and `format.fix` to attempt to automatically fix formatting. It is recommended to check and manually fix formatting as automatic formatting can unintentionally change code.

### Contributing

As an open source project, contributions are welcome and appreciated. Before contributing, please read our [Contribution Guidelines](CONTRIBUTING.md).
