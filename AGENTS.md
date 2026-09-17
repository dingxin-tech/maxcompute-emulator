# Emulator verification

Use Docker containers for compiled-code verification. This workspace's macOS host intercepts newly compiled native executables; do not run the emulator or custom probes directly on the host, including via `go run`. Do not work around interception with signing changes, renamed copies, or repeated execution.

Build the candidate from source using the repository Dockerfile. Run Go tests in the build container, Java SDK/Testcontainers against the candidate image, and C++ probes in their test container. Existing Maven/JDK tools may run on the host with the emulator in Docker. See [build notes](docs/build.md) and [C++ contract probe](tests/cpp/README.md).

Record the tested source commit, image ID/digest and consumer results. A blocked host executable is an environment failure, not a protocol failure or a passing test.

For error-semantics changes, compare the relevant service contract and SDK path before changing behavior. Preserve behavior confirmed by design; distinguish malformed specifications, missing resources and invalid sessions. Keep internal reference implementation details out of public code and documentation.
