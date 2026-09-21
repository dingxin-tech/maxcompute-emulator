import static org.junit.jupiter.api.Assertions.*;

import com.aliyun.odps.*;
import com.aliyun.odps.account.AliyunAccount;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.*;
import org.junit.jupiter.api.*;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

/**
 * Java SDK acceptance for the UDF metadata plane: file-like resources and the
 * functions that reference them. This is the contract pyodps/Java users hit
 * before any UDF can run, so it is verified against the emulator, not mocked.
 */
public class UdfMetadataTest {
  static GenericContainer<?> container;
  static String endpoint;
  Odps odps;

  @BeforeAll
  static void start() {
    endpoint = System.getProperty("emulator.endpoint");
    if (endpoint == null) {
      container =
          new GenericContainer<>(
                  DockerImageName.parse(
                      System.getProperty("emulator.image", "maxcompute/maxcompute-emulator:1.1.0")))
              .withExposedPorts(8080)
              .waitingFor(Wait.forHttp("/readyz"));
      container.start();
      endpoint = "http://" + container.getHost() + ":" + container.getMappedPort(8080);
    }
  }

  @AfterAll
  static void stop() {
    if (container != null) container.stop();
  }

  @BeforeEach
  void client() {
    odps = new Odps(new AliyunAccount("test-ak", "test-sk"));
    odps.setDefaultProject("test_project");
    odps.setCurrentSchema("default");
    odps.setEndpoint(endpoint);
    odps.setTunnelEndpoint(endpoint);
  }

  static String suffix() {
    return UUID.randomUUID().toString().replace("-", "").substring(0, 12);
  }

  static String readAll(InputStream in) throws Exception {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    byte[] buf = new byte[4096];
    int n;
    while ((n = in.read(buf)) > 0) out.write(buf, 0, n);
    return new String(out.toByteArray(), StandardCharsets.UTF_8);
  }

  PyResource upload(String name, String body, String comment) throws Exception {
    PyResource r = new PyResource();
    r.setName(name);
    r.setComment(comment);
    odps.resources().create(r, new ByteArrayInputStream(body.getBytes(StandardCharsets.UTF_8)));
    return r;
  }

  @Test
  void resourceUploadMetadataDownloadAndList() throws Exception {
    String name = "wordcount_" + suffix() + ".py";
    String body = "@annotate('string->string')\nclass WC(object):\n  pass\n";
    upload(name, body, "udf code");

    assertTrue(odps.resources().exists(name), "resource should exist after create");
    Resource got = odps.resources().get(name);
    assertEquals(Resource.Type.PY, got.getType());
    assertEquals("udf code", got.getComment());
    assertEquals((long) body.getBytes(StandardCharsets.UTF_8).length, got.getSize());
    assertEquals("emulator", got.getOwner());
    assertNotNull(got.getCreatedTime());
    assertNotNull(got.getLastModifiedTime());
    assertEquals(name, got.getName());

    try (InputStream in = odps.resources().getResourceAsStream(name)) {
      assertEquals(body, readAll(in));
    }

    String updated = body + "# v2\n";
    PyResource again = new PyResource();
    again.setName(name);
    odps.resources().update(again, new ByteArrayInputStream(updated.getBytes(StandardCharsets.UTF_8)));
    try (InputStream in = odps.resources().getResourceAsStream(name)) {
      assertEquals(updated, readAll(in));
    }
    assertEquals((long) updated.getBytes(StandardCharsets.UTF_8).length,
        odps.resources().get(name).getSize());

    boolean listed = false;
    for (Resource r : odps.resources()) {
      if (name.equalsIgnoreCase(r.getName())) listed = true;
    }
    assertTrue(listed, "listing must contain the uploaded resource");
    assertFalse(odps.resources().exists("missing_" + suffix() + ".py"));

    odps.resources().delete(name);
    assertFalse(odps.resources().exists(name), "resource should be gone after delete");
  }

  @Test
  void duplicateResourceCreateIsRejected() throws Exception {
    String name = "dup_" + suffix() + ".py";
    upload(name, "print(1)", "");
    OdpsException e =
        assertThrows(
            OdpsException.class,
            () -> upload(name, "print(2)", ""),
            "creating the same resource twice must fail");
    assertTrue(e.getMessage().contains("ResourceAlreadyExists"), e.getMessage());
    odps.resources().delete(name);
  }

  @Test
  void functionReferencesResourcesAndCanBeUpdated() throws Exception {
    String code = "udf_" + suffix() + ".py";
    String config = "cfg_" + suffix() + ".py";
    upload(code, "print(1)", "");
    upload(config, "k=1", "");

    Function f = new Function();
    f.setName("run_" + suffix());
    f.setClassType("com.example.WordCount");
    f.setResources(Arrays.asList(code, config));
    odps.functions().create(f);

    assertTrue(odps.functions().exists(f.getName()));
    Function got = odps.functions().get(f.getName());
    assertEquals("com.example.WordCount", got.getClassType());
    assertEquals(2, got.getResources().size());
    assertEquals("emulator", got.getOwner());
    assertNotNull(got.getCreatedTime());

    got.setClassType("com.example.WordCountV2");
    odps.functions().update(got);
    assertEquals("com.example.WordCountV2", odps.functions().get(f.getName()).getClassType());

    boolean listed = false;
    for (Function x : odps.functions()) {
      if (f.getName().equalsIgnoreCase(x.getName())) listed = true;
    }
    assertTrue(listed, "listing must contain the registered function");

    odps.functions().delete(f.getName());
    assertFalse(odps.functions().exists(f.getName()));
    // Dropping the function keeps the resources in place, as on the service.
    assertTrue(odps.resources().exists(code));
    odps.resources().delete(code);
    odps.resources().delete(config);
  }

  @Test
  void functionWithMissingResourceIsRejected() throws Exception {
    Function f = new Function();
    f.setName("broken_" + suffix());
    f.setClassType("com.example.Missing");
    f.setResources(Collections.singletonList("absent_" + suffix() + ".py"));
    OdpsException e =
        assertThrows(OdpsException.class, () -> odps.functions().create(f));
    assertTrue(e.getMessage().contains("unavailable resource"), e.getMessage());
    assertFalse(odps.functions().exists(f.getName()));
  }
}
