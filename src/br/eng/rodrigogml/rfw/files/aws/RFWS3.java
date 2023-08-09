package br.eng.rodrigogml.rfw.files.aws;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import br.eng.rodrigogml.rfw.kernel.beans.Pair;
import br.eng.rodrigogml.rfw.kernel.exceptions.RFWCriticalException;
import br.eng.rodrigogml.rfw.kernel.exceptions.RFWException;
import br.eng.rodrigogml.rfw.kernel.logger.RFWLogger;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.Delete;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.GetObjectTaggingRequest;
import software.amazon.awssdk.services.s3.model.GetObjectTaggingResponse;
import software.amazon.awssdk.services.s3.model.ListObjectVersionsRequest;
import software.amazon.awssdk.services.s3.model.ListObjectVersionsResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsResponse;
import software.amazon.awssdk.services.s3.model.NoSuchBucketException;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectTaggingRequest;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.s3.model.Tag;
import software.amazon.awssdk.services.s3.model.Tagging;
import software.amazon.awssdk.services.s3.paginators.ListObjectVersionsIterable;

/**
 * Description: Classe utilitária de gerenciamento e manuseio do serviço S3 da AWS.<br>
 *
 * @author Rodrigo GML
 * @since 10.0 (24 de out de 2020)
 */
// Códigos baseados nos exemplos em: https://github.com/awsdocs/aws-doc-sdk-examples/tree/master/javav2/example_code/s3/src/main/java/com/example/s3
public class RFWS3 {

  /**
   * Hash com as instâncias do Cliente S3. Uma instância é criada por Região e a instância reutilizada.<br>
   * A chave da hash é:
   * <li>Region.toString(); - quando a instância é solicitada apenas pela região
   * <li>key + "|" + secret + "|" + region.toString(); - quando a instância é solicitada com chave e secret.
   */
  private static final HashMap<String, RFWS3> clientInstancesHash = new HashMap<>();

  /**
   * Objeto referência para manipulzar o S3.
   */
  private final S3Client s3Client;

  /**
   * Cria um objeto para se autenticar no S3 aravés {@link DefaultCredentialsProvider}.<br>
   * O provedor padrão procura uma autenticação em cadeia conforme descrito em https://docs.aws.amazon.com/sdk-for-java/v2/developer-guide/credentials.html
   *
   * @param region Região da AWS em que as operações serão executadas
   * @throws RFWException
   */
  private RFWS3(Region region) throws RFWException {
    try {
      this.s3Client = S3Client.builder().region(region).build();
    } catch (NoSuchKeyException e) {
      throw new RFWCriticalException("RFW_000023", e);
    } catch (Throwable e) {
      throw new RFWCriticalException("RFW_000022", e);
    }
  }

  /**
   * Cria um objeto para se autenticar no S3 aravés do conjunto de chave/secret
   *
   * @param key Chave de Acesso à conta da AWS
   * @param secret Senha de Acesso da conta
   * @param region Região da AWS em que as operações serão executadas
   * @throws RFWException
   */
  private RFWS3(String key, String secret, Region region) throws RFWException {
    try {
      final AwsBasicCredentials awsCreds = AwsBasicCredentials.create(key, secret);
      final StaticCredentialsProvider credProvider = StaticCredentialsProvider.create(awsCreds);

      this.s3Client = S3Client.builder().region(region).credentialsProvider(credProvider).build();
    } catch (NoSuchKeyException e) {
      throw new RFWCriticalException("RFW_000023", e);
    } catch (Throwable e) {
      throw new RFWCriticalException("RFW_000022", e);
    }
  }

  /**
   * Obtém uma instância para determinada região que usará a autenticação "por máquina" da AWS. Mais útil e simples quando a aplicação roda na estrutura da própria AWS.
   *
   * @param region Região de acesso do S3.
   * @return Instância do {@link RFWS3} pronta para acessar a AWS.
   * @throws RFWException
   */
  public static RFWS3 getInstance(Region region) throws RFWException {
    RFWS3 rfws3 = clientInstancesHash.get(region.toString());
    if (rfws3 == null) {
      rfws3 = new RFWS3(region);
      clientInstancesHash.put(region.toString(), rfws3);
    }
    return rfws3;
  }

  /**
   * Obtém uma instância para determinada região que usará a autenticação "por máquina" da AWS. Mais útil e simples quando a aplicação roda na estrutura da própria AWS.
   *
   * @param key Chave de Acesso à conta da AWS
   * @param secret Senha de Acesso da conta
   * @param region Região de acesso do S3.
   * @return Instância do {@link RFWS3} pronta para acessar a AWS.
   * @throws RFWException
   */
  public static RFWS3 getInstance(String key, String secret, Region region) throws RFWException {
    RFWS3 rfws3 = clientInstancesHash.get(key + "|" + secret + "|" + region.toString());
    if (rfws3 == null) {
      rfws3 = new RFWS3(key, secret, region);
      clientInstancesHash.put(key + "|" + secret + "|" + region.toString(), rfws3);
    }
    return rfws3;
  }

  /**
   * Envia um arquivo para o Bucket do S3.
   *
   * @param bucket nome do Bucket para escrita do arquivo.
   * @param filePath Caminho dentro do bucket e nome do arquivo. Diretórioa são criados automaticamente. Utiliza o separador '/'.
   * @param file Arquivo que será feito o upload.
   * @return Objeto recebido direto do S3 com as informações do arquivo enviado.
   */
  public PutObjectResponse putObject(String bucket, String filePath, File file) throws RFWException {
    try {
      // long t = System.currentTimeMillis();
      PutObjectRequest request = PutObjectRequest.builder().bucket(bucket).key(filePath).build();
      PutObjectResponse response = this.s3Client.putObject(request, file.getAbsoluteFile().toPath());
      // t = System.currentTimeMillis() - t;
      // System.out.println("################################################ File Send: " + filePath + " / Instance: " + this.toString() + " / Time [ms]: " + t);
      return response;
    } catch (NoSuchKeyException e) {
      throw new RFWCriticalException("RFW_000023", e);
    } catch (Throwable e) {
      throw new RFWCriticalException("RFW_000022", e);
    }
  }

  /**
   * Recupera um arquivo no Bucket do S3 salvando em um {@link File} do sistema. Normalmente um arquivo temporário do sistema.
   *
   * @param bucket nome do Bucket para escrita do arquivo.
   * @param objectKeyName nome do Objeto no S3. Caminho completo do arquivo separados por '/'.
   * @param versionID Identificador da Versão do arquivo do Bucket.
   * @param file Arquivo que será feito o upload.
   * @return Resposta da API da AWS
   * @throws RFWException Lançado em Caso de falhas:
   *           <li>RFW_000023 - Caso o objeto não seja encontrado.
   */
  public GetObjectResponse getObject(String bucket, String objectKeyName, String versionID, File file) throws RFWException {
    try {
      final software.amazon.awssdk.services.s3.model.GetObjectRequest.Builder builder = GetObjectRequest.builder().bucket(bucket).key(objectKeyName);
      if (versionID != null) builder.versionId(versionID);
      GetObjectRequest request = builder.build();
      return this.s3Client.getObject(request, file.toPath());
    } catch (NoSuchKeyException e) {
      throw new RFWCriticalException("RFW_000023", e);
    } catch (Throwable e) {
      throw new RFWCriticalException("RFW_000022", e);
    }
  }

  /**
   * Excluí um objeto no Bucket do S3. Ao excluir um objeto, se o Bucket for versionado o S3 adicionará a "marca de deleção" no arquivo, deixando a versão do arquivo, só marcando que está excluído.<br>
   * Para excluir definiticamente é preciso utilizar o método de excluir versão, informando a versão do arquivo para exclusão.
   *
   * @param bucketName Nome do Bucket containar do objeto
   * @param objectKeyNames Nomes/Nome do Objeto no S3. Caminho completo do arquivo separados por '/'.
   */
  public void deleteObjects(String bucketName, String... objectKeyNames) throws RFWException {
    try {
      final ArrayList<ObjectIdentifier> toDelete = new ArrayList<ObjectIdentifier>();
      for (String objectName : objectKeyNames) {
        toDelete.add(ObjectIdentifier.builder().key(objectName).build());
      }

      DeleteObjectsRequest dor = DeleteObjectsRequest.builder()
          .bucket(bucketName)
          .delete(Delete.builder().objects(toDelete).build())
          .build();
      this.s3Client.deleteObjects(dor);
    } catch (NoSuchKeyException e) {
      throw new RFWCriticalException("RFW_000023", e);
    } catch (Throwable e) {
      throw new RFWCriticalException("RFW_000022", e);
    }
  }

  /**
   * Excluí uma versão de objeto no Bucket do S3.
   *
   * @param bucketName Nome do Bucket containar do objeto
   * @param objectKey Nome (Caminho completo do arquivo separados por '/') do Objeto no S3.
   * @param version versão do arquivo para exclusão.
   */
  public void deleteObjectsVersion(String bucketName, String objectKey, String version) throws RFWException {
    List<Pair<String, String>> list = new ArrayList<Pair<String, String>>(1);
    list.add(new Pair<String, String>(objectKey, version));
    deleteObjectsVersion(bucketName, list);
  }

  /**
   * Excluí uma versão de objeto no Bucket do S3.
   *
   * @param bucketName Nome do Bucket containar do objeto
   * @param objectKeyNames Par de valores sendo a chave o nome (Caminho completo do arquivo separados por '/') do Objeto no S3 e o valor a versão do arquivo para exclusão.
   */
  public void deleteObjectsVersion(String bucketName, List<Pair<String, String>> objectKeyNames) throws RFWException {
    try {
      final ArrayList<ObjectIdentifier> toDelete = new ArrayList<ObjectIdentifier>();
      for (Pair<String, String> pair : objectKeyNames) {
        toDelete.add(ObjectIdentifier.builder().key(pair.getKey()).versionId(pair.getValue()).build());
      }

      DeleteObjectsRequest dor = DeleteObjectsRequest.builder()
          .bucket(bucketName)
          .delete(Delete.builder().objects(toDelete).build())
          .build();
      this.s3Client.deleteObjects(dor);
    } catch (NoSuchKeyException e) {
      throw new RFWCriticalException("RFW_000023", e);
    } catch (Throwable e) {
      throw new RFWCriticalException("RFW_000022", e);
    }
  }

  /**
   * Recupera as Tags definidas no objeto dentro do bucket do S3
   *
   * @param bucketName
   * @param objectKeyName nome do Objeto no S3. Caminho completo do arquivo separados por '/'.
   * @return
   */
  public Map<String, String> getObjectTags(String bucketName, String objectKeyName) throws RFWException {
    try {
      GetObjectTaggingRequest getTaggingRequest = GetObjectTaggingRequest
          .builder()
          .key(objectKeyName)
          .bucket(bucketName)
          .build();

      GetObjectTaggingResponse tags = this.s3Client.getObjectTagging(getTaggingRequest);
      List<Tag> tagSet = tags.tagSet();

      return tagSet.stream().map(tag -> new Pair<String, String>(tag.key(), tag.value())).collect(Collectors.toMap(Pair::getKey, Pair::getValue));
    } catch (NoSuchKeyException e) {
      throw new RFWCriticalException("RFW_000023", e);
    } catch (Throwable e) {
      throw new RFWCriticalException("RFW_000022", e);
    }
  }

  /**
   * Recupera as Tags definidas no objeto dentro do bucket do S3
   *
   * @param bucketName
   * @param objectKeyName nome do Objeto no S3. Caminho completo do arquivo separados por '/'.
   * @return
   */
  public void putObjectTags(String bucketName, String objectKeyName, Map<String, String> tags) throws RFWException {
    try {
      Tag[] tagSet = new Tag[tags.size()];
      int c = 0;
      for (Entry<String, String> tag : tags.entrySet()) {
        tagSet[c++] = Tag.builder().key(tag.getKey()).value(tag.getValue()).build();
      }
      Tagging tagging = Tagging.builder().tagSet(tagSet).build();

      PutObjectTaggingRequest putTaggingRequest = PutObjectTaggingRequest
          .builder()
          .tagging(tagging)
          .key(objectKeyName)
          .bucket(bucketName)
          .build();

      this.s3Client.putObjectTagging(putTaggingRequest);
    } catch (NoSuchKeyException e) {
      throw new RFWCriticalException("RFW_000023", e);
    } catch (Throwable e) {
      throw new RFWCriticalException("RFW_000022", e);
    }
  }

  /**
   * Lista todos os objetos de um determinado Bucket.<Br>
   *
   * @param bucketName
   * @return
   * @throws RFWException
   */
  public List<S3Object> listObjects(String bucketName) throws RFWException {
    try {
      ListObjectsRequest listObjects = ListObjectsRequest
          .builder()
          .bucket(bucketName)
          .build();
      ListObjectsResponse res = s3Client.listObjects(listObjects);
      return res.contents();
    } catch (NoSuchBucketException e) {
      throw new RFWCriticalException("RFW_000024", e);
    } catch (Throwable e) {
      throw new RFWCriticalException("RFW_000022", e);
    }
  }

  /**
   * Defindo a grande quantidade possível de itens dentro de um bucket este método não retorna um objeto, mas sim um stream que permite que o AWS baixe as informações a medida que a iteração ocorre.<br>
   * <b>NÃO TENTE SALVAR TODOS OS OBJETOS, PODE FALTAR MEMÓRIA DEPENDENDO DA QUANTIADDE DE ITENS.</B> <BR>
   * É possível iterar o objeto de três manteiras diferentes: <br>
   * <li>Using a Stream
   *
   * <pre>
   * responses.stream().forEach(....);
   * </pre>
   *
   * <li>Using For loop</li>
   *
   * <pre>
   * for ( {@link ListObjectVersionsResponse} response : responses) { // do something;  }
   * </pre>
   *
   * <li>Use iterator directly
   *
   * <pre>
   * responses.iterator().forEachRemaining(....);
   * </pre>
   *
   * @param bucketName Nome do Bucket a ser listado
   * @param prefix Prefixo do nome dos objetos. pode ser utilizado para filtrar os resultados por diretório, por exemplo "bis_kernel/nfe/".
   * @return
   * @throws RFWException
   */
  public ListObjectVersionsIterable listObjectsVersionsIterable(String bucketName, String prefix) throws RFWException {
    ListObjectVersionsRequest request = ListObjectVersionsRequest
        .builder()
        .bucket(bucketName)
        .prefix(prefix)
        .build();

    return s3Client.listObjectVersionsPaginator(request);
  }

  @Override
  protected void finalize() throws Throwable {
    super.finalize();

    // Força a finalização de todos os clientes caso, por algum motivo, a classe seja jogada fora. O que não deve acontecer por ser estática.
    for (RFWS3 rfws3 : clientInstancesHash.values()) {
      try {
        rfws3.s3Client.close();
      } catch (Throwable t) {
        RFWLogger.logException(t);
      }
    }
  }

}
