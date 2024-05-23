# MaxCompute Emulator

## Introduction

MaxCompute Emulator is a lightweight utility designed to simulate the behavior of Alibaba Cloud's MaxCompute (previously known as ODPS) service. It leverages SQLite to offer a mock environment for clients who don’t have access to an actual MaxCompute account to test their functionalities. This project is particularly useful for development and testing purposes where MaxCompute resource accessibility or cost constraints are a concern.

## Installation

The project is a typical SpringBoot application, which can be run directly with the following command:
```bash
# Clone the repository
git clone https://github.com/dingxin-tech/maxcompute-emulator.git

# Navigate to the emulator directory
cd maxcompute-emulator

# Build the project
./mvnw install

# Run the project
./mvnw spring-boot:run
```

## Current State

The project is currently in the **pre-alpha phase**, with main interfaces implemented, including but not limited to:

1. **SQL-related Interfaces**
2. **Table Metadata-related Interfaces**
3. **Tunnel upsert Interfaces**

## Contributing

Your contributions to enhance `MaxCompute Emulator` are welcome! If you would like to contribute to the project, please follow these steps:

1. Fork the repository.
2. Create a new branch for each feature or improvement.
3. Submit a pull request with comprehensive description of changes.

## Support

If you encounter any issues or require assistance, please open an issue on the project's GitHub issues page.

## License

This project is licensed under the [Apache 2.0] License - see the LICENSE file for details.