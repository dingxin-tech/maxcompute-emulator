# MaxCompute Emulator

## Introduction

MaxCompute Emulator is a lightweight utility designed to simulate the behavior of Alibaba Cloud's MaxCompute (previously known as ODPS) service. 

It leverages SQLite to offer a mock environment for clients who don’t have access to an actual MaxCompute account to test their functionalities. 

This project is particularly useful for development and testing purposes where MaxCompute resource accessibility or cost constraints are a concern.

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

Or you can use the docker image:
```bash
docker run -p 8080:8080 maxcompute/maxcompute-emulator:latest
```
## Usage
When you want to implement e2e testing, it is recommended to use it in conjunction with the [TestContainers](https://java.testcontainers.org/) component. 

Usage examples are as follows

```java
// ignore imports

public class MaxComputeEmulatorTest {

    public static final DockerImageName MAXCOMPUTE_IMAGE =
            DockerImageName.parse("maxcompute/maxcompute-emulator:v0.0.1");

    @ClassRule
    public static GenericContainer<?> maxcompute =
            new GenericContainer<>(MAXCOMPUTE_IMAGE)
                    .withExposedPorts(8080)
                    .waitingFor(
                            Wait.forLogMessage(".*Started MaxcomputeEmulatorApplication.*\\n", 1));

    public Odps getTestOdps() {
        Account account = new AliyunAccount("ak", "sk");
        Odps odps = new Odps(account);
        odps.setEndpoint(getEndpoint());
        odps.setTunnelEndpoint(getEndpoint());
        return odps;
    }

    @Test
    public void test() {
        Odps odps = getTestOdps();
        // create table and get schema for example
        Instance instance = SQLTask.run(odps, "create table test(c1 bigint)");
        instance.waitForSuccess();
        System.out.println(odps.tables().get("test").getSchema());
    }

    private String getEndpoint() {
        String ip;
        if (maxcompute.getHost().equals("localhost")) {
            try {
                ip = InetAddress.getLocalHost().getHostAddress();
            } catch (UnknownHostException e) {
                ip = "127.0.0.1";
            }
        } else {
            ip = maxcompute.getHost();
        }
        return "http://" + ip + ":" + maxcompute.getFirstMappedPort();
    }
}
```
**Note**: When using some interfaces that need to return the current server IP (Tunnel related interfaces), 
the server returns `127.0.0.1` by default. However, in a container environment, the server's IP and port may be random, 
so we provide an interface to manually upload the IP and port. 

The sample code for using this interface is as follows
```java
public class MaxComputeEmulatorTest {
    
    // other code as previous, here ignore
    
    @Test
    public void test() throws Exception {
        sendPOST(getEndpoint() + "/init", getEndpoint());
        // other logic
        
    }
    
    // here use java http client to send post, okhttp or other http client can also be used
    public static void sendPOST(String postUrl, String postData) throws Exception {
        URL url = new URL(postUrl);

        HttpURLConnection httpURLConnection = (HttpURLConnection) url.openConnection();
        httpURLConnection.setRequestMethod("POST");
        httpURLConnection.setDoOutput(true);
        httpURLConnection.setRequestProperty("Content-Type", "application/json");
        httpURLConnection.setRequestProperty("Content-Length", String.valueOf(postData.length()));

        try (OutputStream outputStream = httpURLConnection.getOutputStream()) {
            outputStream.write(postData.getBytes("UTF-8"));
            outputStream.flush();
        }
        int responseCode = httpURLConnection.getResponseCode();
        if (responseCode != HttpURLConnection.HTTP_OK) {
            throw new RuntimeException("POST request failed with response code: " + responseCode);
        }
    }
}
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