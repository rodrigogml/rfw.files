package br.eng.rodrigogml.rfw.files.aws;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;

import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import br.eng.rodrigogml.rfw.kernel.RFW;
import br.eng.rodrigogml.rfw.kernel.beans.Pair;
import br.eng.rodrigogml.rfw.kernel.exceptions.RFWException;
import br.eng.rodrigogml.rfw.kernel.preprocess.PreProcess;
import br.eng.rodrigogml.rfw.kernel.utils.RUFile;
import br.eng.rodrigogml.rfw.kernel.utils.RUGenerators;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectVersionsResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsResponse;
import software.amazon.awssdk.services.s3.model.ObjectVersion;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.s3.paginators.ListObjectVersionsIterable;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class RFWS3Test {

  private static final String awsTestFolder = "testRun/";
  private static final String awsTestBucker = "biserp.dev";
  public static String awsTestSecret = null;
  public static String awsTestKey = null;
  public static Region awsTestRegion = null;

  @Before
  public void init() throws Exception {
    awsTestKey = RFW.getDevProperty("aws.s3.key");
    awsTestSecret = RFW.getDevProperty("aws.s3.secret");
    PreProcess.requiredNonNull(awsTestKey, "Chave de acesso ao S3 não definida para realizar o teste!");
    PreProcess.requiredNonNull(awsTestSecret, "Chave de acesso ao S3 não definida para realizar o teste!");
    awsTestRegion = Region.US_EAST_1;
  }

  @Test
  public void t00_directBucketUploadTest() throws Exception {
    StaticCredentialsProvider cred = StaticCredentialsProvider.create(AwsBasicCredentials.create(awsTestKey, awsTestSecret));
    S3Client s3 = S3Client.builder().region(awsTestRegion).credentialsProvider(cred).build();

    PutObjectRequest putObjectRequest = PutObjectRequest.builder()
        .bucket(awsTestBucker)
        .key(awsTestFolder + RUGenerators.generateUUID() + ".pdf")
        .build();

    File file = new File(RFWS3Test.class.getResource("/resources/MySamplePDFFile.pdf").toURI());
    s3.putObject(putObjectRequest, file.getAbsoluteFile().toPath());
  }

  @Test
  public void t01_directBucketDeleteTest() throws Exception {
    StaticCredentialsProvider cred = StaticCredentialsProvider.create(AwsBasicCredentials.create(awsTestKey, awsTestSecret));
    S3Client s3 = S3Client.builder().region(awsTestRegion).credentialsProvider(cred).build();

    final String filePathKey = awsTestFolder + RUGenerators.generateUUID() + ".pdf";

    PutObjectRequest putObjectRequest = PutObjectRequest.builder()
        .bucket(awsTestBucker)
        .key(filePathKey)
        .build();

    File file = new File(RFWS3Test.class.getResource("/resources/MySamplePDFFile.pdf").toURI());
    s3.putObject(putObjectRequest, file.getAbsoluteFile().toPath());

    DeleteObjectRequest deleteObjectRequest = DeleteObjectRequest.builder()
        .bucket(awsTestBucker)
        .key(filePathKey)
        .build();

    // Delete the object
    s3.deleteObject(deleteObjectRequest);
  }

  @Test
  public void t02_bucketObjectOperations() throws Exception {
    final String objectKeyName = awsTestFolder + "MySamplePDFFile.pdf";
    final String bucketName = awsTestBucker;

    final RFWS3 s3 = RFWS3.getInstance(awsTestKey, awsTestSecret, awsTestRegion);

    // ### Testa enviar o arquivo
    PutObjectResponse postResp = s3.putObject(bucketName, objectKeyName, new File(RFWS3Test.class.getResource("/resources/MySamplePDFFile.pdf").toURI()));
    assertNotNull(postResp.versionId());

    // ### Testa recuperar o arquivo
    final File file = RUFile.createFileInTemporaryPath("MySamplePDFFile.pdf");
    s3.getObject(bucketName, objectKeyName, postResp.versionId(), file);
    assertTrue(file.exists());
    assertTrue(file.length() > 0);

    // ### Testa criar tags para o arquivo
    Map<String, String> tags = new HashMap<String, String>();
    tags.put("tag1", RUGenerators.generateString(15));
    tags.put("tag2", RUGenerators.generateString(35));
    s3.putObjectTags(bucketName, objectKeyName, tags);

    // ### Testa recuperar as tags para o arquivo
    Map<String, String> tags2 = s3.getObjectTags(bucketName, objectKeyName);
    for (Entry<String, String> entry : tags.entrySet()) {
      // Validamos se todas as chaves enviadas voltaram e não o contrário, pois podemos ter tags automáticas do s3 que não foram enviadas daqui.
      String v = tags2.get(entry.getKey());
      assertEquals(entry.getValue(), v);
    }

    // ### Lista os arquivos do Bucket e verifica se o encontramos na listagem (testa o método de listagem)
    boolean found = false;
    for (S3Object s3Obj : s3.listObjects(bucketName)) {
      if (s3Obj.key().equals(objectKeyName)) {
        found = true;
        break;
      }
    }
    if (!found) fail("O arquivo enviado para o S3 não apareceu na listagem de arquivos.");
    // ### Testa excluir o objeto
    final File file2 = RUFile.createFileInTemporaryPath("MySamplePDFFile.pdf");
    s3.deleteObjects(bucketName, objectKeyName);

    // Tenta recuperar para ver se o objeto é retornado
    try {
      s3.getObject(bucketName, objectKeyName, null, file2);
      fail("O Objeto excluído do S3 foi encontrado!");
    } catch (RFWException e) {
      if (!"RFW_000023".equals(e.getExceptionCode())) {
        fail("O arquivo foi encontrado ou não obtivemos a exception prometida!");
      }
    }
  }

  @Test
  public void t03_listBucketObjectVersions() throws Exception {
    final String bucketName = awsTestBucker;
    final String objectKeyName = awsTestFolder + "t01TestCase.pdf";
    final RFWS3 s3 = RFWS3.getInstance(awsTestKey, awsTestSecret, awsTestRegion);

    // ### Enviar o arquivo N vezes o arquivo para criar 4 versões
    String[] versions = new String[5];
    for (int i = 0; i < versions.length; i++) {
      PutObjectResponse postResp = s3.putObject(bucketName, objectKeyName, new File(RFWS3Test.class.getResource("/resources/MySamplePDFFile.pdf").toURI()));
      versions[i] = postResp.versionId();
    }

    // ### Recupera as versões dele e confirme que foram feitas 5 versões
    {
      ListObjectVersionsIterable list = s3.listObjectsVersionsIterable(bucketName, objectKeyName);
      for (ListObjectVersionsResponse objResp : list) {
        assertEquals("Quantidade de versões do arquivo diferente da esperada!", versions.length, objResp.versions().size());
      }
    }

    // ### Tentamos excluir as duas versões mais antigas primeiro
    final ArrayList<Pair<String, String>> deleteList = new ArrayList<Pair<String, String>>();
    deleteList.add(new Pair<String, String>(objectKeyName, versions[0]));
    deleteList.add(new Pair<String, String>(objectKeyName, versions[1]));
    s3.deleteObjectsVersion(bucketName, deleteList);

    // Validamos se as versões que sobraram as as 3/4/5
    {
      ListObjectVersionsIterable list = s3.listObjectsVersionsIterable(bucketName, objectKeyName);
      HashSet<String> foundVers = new HashSet<String>();
      for (ListObjectVersionsResponse objResp : list) {
        for (ObjectVersion objVer : objResp.versions()) {
          foundVers.add(objVer.versionId());
        }
      }
      assertEquals("Quantidade de versões remanescentes do arquivo diferente da esperada!", foundVers.size(), versions.length - 2);
    }

    // ### Excluímos o restante das versões e garantimos que não sobrou nada.
    deleteList.clear();
    deleteList.add(new Pair<String, String>(objectKeyName, versions[2]));
    deleteList.add(new Pair<String, String>(objectKeyName, versions[3]));
    deleteList.add(new Pair<String, String>(objectKeyName, versions[4]));
    s3.deleteObjectsVersion(bucketName, deleteList);

    // Validamos se as versões que sobraram as as 3/4/5
    {
      ListObjectVersionsIterable list = s3.listObjectsVersionsIterable(bucketName, objectKeyName);
      HashSet<String> foundVers = new HashSet<String>();
      for (ListObjectVersionsResponse objResp : list) {
        for (ObjectVersion objVer : objResp.versions()) {
          foundVers.add(objVer.versionId());
        }
      }
      assertEquals("Era esperado que não sobrasse nenhuma versão do arquivo no bucket!", foundVers.size(), 0);
    }
  }

  @Test
  public void t99_directCleanTestFiles() throws Exception {
    StaticCredentialsProvider cred = StaticCredentialsProvider.create(AwsBasicCredentials.create(awsTestKey, awsTestSecret));
    S3Client s3 = S3Client.builder().region(awsTestRegion).credentialsProvider(cred).build();

    ListObjectsRequest listObjectsRequest = ListObjectsRequest.builder()
        .bucket(awsTestBucker)
        .prefix(awsTestFolder)
        .build();

    ListObjectsResponse objectListing = s3.listObjects(listObjectsRequest);

    for (S3Object object : objectListing.contents()) {
      s3.deleteObject(DeleteObjectRequest.builder()
          .bucket(awsTestBucker)
          .key(object.key())
          .build());
    }
  }
}
