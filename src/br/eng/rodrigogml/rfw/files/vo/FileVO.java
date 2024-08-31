package br.eng.rodrigogml.rfw.files.vo;

import java.time.LocalDateTime;

import br.eng.rodrigogml.rfw.files.vo.FileVO.FileCompression;
import br.eng.rodrigogml.rfw.files.vo.FileVO.FilePersistenceType;
import br.eng.rodrigogml.rfw.kernel.dataformatters.LocaleConverter;
import br.eng.rodrigogml.rfw.kernel.preprocess.PreProcess.PreProcessOption;
import br.eng.rodrigogml.rfw.kernel.rfwmeta.RFWMetaDateField;
import br.eng.rodrigogml.rfw.kernel.rfwmeta.RFWMetaDateField.DateResolution;
import br.eng.rodrigogml.rfw.kernel.rfwmeta.RFWMetaEnumField;
import br.eng.rodrigogml.rfw.kernel.rfwmeta.RFWMetaLongField;
import br.eng.rodrigogml.rfw.kernel.rfwmeta.RFWMetaRelationshipField;
import br.eng.rodrigogml.rfw.kernel.rfwmeta.RFWMetaRelationshipField.RelationshipTypes;
import br.eng.rodrigogml.rfw.kernel.rfwmeta.RFWMetaStringField;
import br.eng.rodrigogml.rfw.kernel.utils.RUGenerators;
import br.eng.rodrigogml.rfw.kernel.vo.RFWVO;

/**
 * Description: Classe que representa um arquivo utilizado pelo sistema.<br>
 * O Core do bis provê um sistema de armazemamento de arquivos diretamente no banco de dados para simplificar a vida dos desenvolvedores de módulos.<br>
 *
 * @author Rodrigo Leitão
 * @since 3.1.0 (DEZ / 2009)
 */
@SuppressWarnings("unused")
public class FileVO extends RFWVO {

  private static final long serialVersionUID = 938608718161221863L;

  /**
   * Define o tipo de persistência do arquivo.
   */
  public static enum FilePersistenceType {
    /**
     * Indica que o conteúdo do arquivo é persistido no Banco de Dados.
     */
    DB,
    /**
     * Indica que o conteúdo do arquivo é persistido e versionado no AWS S3.
     */
    S3
  }

  /**
   * Modelos de compressão do conteúdo suportados
   */
  public static enum FileCompression {

    /**
     * O conteúdo do arquivo não está comprimido.<br>
     * Este modo é interessante quando o arquivo é muito utilizado, assim tiramos o overhead de processamento de descoprimi-lo o tempo todo.
     */
    NONE,
    /**
     * Metodo de máximo de compressão do BIS, utiliza a compressão no máximo para diminuir o espaço utilizado em disco, mesmo que utilize mais processamento.<br>
     * Este modo é útil quando os arquivos serão guardados por longo período de tempo e serão acessado poucas vezes.<Br>
     * <br>
     * Obs: para ajudar a recuperar o arquivo original, indepentende do moto de compressão utilizado, verifique o método {@link B10File#processFileVOTempFile(FileVO)}.
     */
    MAXIMUM_COMPRESSION,
  }

  /**
   * Nome do arquivo salvo. <br>
   * Mesmo que no modo {@link FilePersistenceType#S3} o arquivo seja comprimido (enviado como .zip para o S3) aqui a extenção e nome do arquivo continuaram intactos. No momento de recuperar o arquivo do S3 o valor de {@link FileCompression} é avaliado para determinar a extenção correta a ser utilizada
   */
  @RFWMetaStringField(caption = "Arquivo", maxLength = 255, required = true, pattern = "[^\\\\/<>\\?:\\|\\*\\\"]+\\.[^\\\\/<>\\?:\\|\\*\\\"]{1,4}")
  private String name = null;

  /**
   * Tag com o nome do módulo dono do arquivo. Esta tag ajuda em caso de problemas a identificar o arquivo e encontra-lo.<br>
   * Caso seja salvo no S3, esse valor é colocado como o nome de um folder.<Br>
   * <br>
   * Obs1: Os TagIDs do sistema ficam registrados como constantes em {@link BISSystem} com o prefixo "FILETAG_". Exemplo: {@link BISSystem#FILETAG_NFE_XML}.<br>
   * Obs2: Considere utilizar o método {@link B10File#createFileVO(FilePersistenceType, byte[], String, String, String, FileCompression)} ou similar para gerar o FileVO.
   */
  @RFWMetaStringField(caption = "TagID", maxLength = 15, pattern = "[\\w ]+", required = true)
  private String tagID = null;

  /**
   * Para arquivos persistidos no S3 não utilizaremos o nome do arquivo pois precisamos garantir um identificador único para que os arquivos não se sobreponham. Assim geramos um UUID que identifica o arquivo. No S3 esse UUID é utilizado como nome do arquivo.
   */
  @RFWMetaStringField(caption = "UUID do Arquivo", maxLength = 36, required = false, pattern = RUGenerators.UUID_REGEXP)
  private String fileUUID = null;

  /**
   * Indica se o conteúdo do arquivo está comprimido ou não.<Br>
   * Quando comprimido é preciso descoprimir o conteúdo do arquivo antes de utiliza-lo.
   */
  @RFWMetaEnumField(caption = "Compressão", required = true)
  private FileCompression compression = null;

  /**
   * Data de criação do arquivo.
   */
  @RFWMetaDateField(caption = "Data Criação", resolution = DateResolution.SECOND, required = true)
  private LocalDateTime dateCreation = null;

  /**
   * Data de modificação do arquivo.
   */
  @RFWMetaDateField(caption = "Data Modificação", resolution = DateResolution.SECOND, required = true)
  private LocalDateTime dateModification = null;

  /**
   * Tamanho do arquivo em bytes (para formatar o valor cheque a classe {@link LocaleConverter})
   */
  @RFWMetaLongField(caption = "Tamanho", minvalue = 0, required = true)
  private Long size = null;

  /**
   * Encoding do conteúdo do arquivo. Geralmente utilizado para arquivos de Texto.
   */
  @RFWMetaStringField(caption = "Encoding", maxLength = 15, required = false)
  private String encoding = null;

  /**
   * Local onde o arquivo é persistido.
   */
  @RFWMetaEnumField(caption = "Tipo de Persistência", required = true)
  private FilePersistenceType persistenceType = null;

  /**
   * Conteúdo do arquivo quando persistido no banco de dados {@link FilePersistenceType} = {@link FilePersistenceType#DB}.<br>
   */
  @RFWMetaRelationshipField(caption = "Conteúdo", relationship = RelationshipTypes.COMPOSITION, required = false)
  private FileContentVO fileContentVO = null;

  /**
   * Chave de versão do arquivo quando no S3. Obrigatório quando {@link FilePersistenceType} = {@link FilePersistenceType#S3}.<br>
   * <b>ATENÇÃO:</B> Quando o FileVO tiver o conteúdo do arquivo alterado, e tivermos de postar um novo arquivo o versionID deve ser definido para nulo. o CRUD realizará um novo post no S3 quando o versionID for nulo. Caso contrário assume que o arquivo continua sendo a versão passada.
   */
  @RFWMetaStringField(caption = "ID Versão S3", maxLength = 50, required = false)
  private String versionID = null;

  /**
   * Caminho base do arquivo (pastas).<br>
   * Este caminho será utilizado ao salvar o arquivo no AWS, permitindo que o sistema separe em pastas os arquivos conforme achar conveniente. Por exemplo, por cliente, tipo de arquivo, etc.<br>
   * O caminho deve ser escrito no formato: "folder/subfolder1/.../subfolderN/" SEMPRE TERMINANDO COM "/"
   */
  @RFWMetaStringField(caption = "Caminho", maxLength = 100, required = false, preProcess = PreProcessOption.STRING_SPACESCLEAN_TO_NULL)
  private String basePath = null;

  /**
   * Caminho para o arquivo temporário onde encontramos o arquivo quando {@link FilePersistenceType} = {@link FilePersistenceType#S3}.<br>
   * Este atributo não é persistido no banco de dados pois é um caminho temporário.<br>
   * Para persistir o FileVO no S3 este atributo é obrigatório, pois o arquivo que será postado no S3 será lido desse caminho.<br>
   * Quando o FileVO for recuperado do banco de dados este atributo virá nulo. Após chamar o método de recuperação do arquivo no S3 o caminho para o arquivo baixado deve ser salvo aqui.<br>
   * Note que o Crud não verifica o conteúdo do arquivo para determinar se houve ou não alteração. Para que o CRUD saiba que o conteúdo do arquivo mudou o conteúdo de {@link #versionID} deve ser definido como null.<br>
   * O conteúdo do arquivo tempPath deve sempre acompanhar o valor definido em {@link #compression}. Ou seja, se ouver compressão o conteúdo deste arquivo já deve estar comprimido conforme a definição, se não houver, deverá ser o arquivo "pleno", e assim por diante. Assim, <b>Cuidado ao lêr o conteúdo deste arquivo</b> pois ele pode estar comprimido e precisa de tratamento antes.
   */
  // SEM @BISMETA PQ ESSE ATRIBUTO NÂO É PERSISTIDO
  private String tempPath = null;

  /**
   * # nome do arquivo salvo. <br>
   * Mesmo que no modo {@link FilePersistenceType#S3} o arquivo seja comprimido (enviado como .zip para o S3) aqui a extenção e nome do arquivo continuaram intactos. No momento de recuperar o arquivo do S3 o valor de {@link FileCompression} é avaliado para determinar a extenção correta a ser utilizada.
   *
   * @return the nome do arquivo salvo
   */
  public String getName() {
    return name;
  }

  /**
   * # nome do arquivo salvo. <br>
   * Mesmo que no modo {@link FilePersistenceType#S3} o arquivo seja comprimido (enviado como .zip para o S3) aqui a extenção e nome do arquivo continuaram intactos. No momento de recuperar o arquivo do S3 o valor de {@link FileCompression} é avaliado para determinar a extenção correta a ser utilizada.
   *
   * @param name the new nome do arquivo salvo
   */
  public void setName(String name) {
    this.name = name;
  }

  /**
   * # data de criação do arquivo.
   *
   * @return the data de criação do arquivo
   */
  public LocalDateTime getDateCreation() {
    return dateCreation;
  }

  /**
   * # data de criação do arquivo.
   *
   * @param dateCreation the new data de criação do arquivo
   */
  public void setDateCreation(LocalDateTime dateCreation) {
    this.dateCreation = dateCreation;
  }

  /**
   * # data de modificação do arquivo.
   *
   * @return the data de modificação do arquivo
   */
  public LocalDateTime getDateModification() {
    return dateModification;
  }

  /**
   * # data de modificação do arquivo.
   *
   * @param dateModification the new data de modificação do arquivo
   */
  public void setDateModification(LocalDateTime dateModification) {
    this.dateModification = dateModification;
  }

  /**
   * # tamanho do arquivo em bytes (para formatar o valor cheque a classe {@link LocaleConverter}).
   *
   * @return the tamanho do arquivo em bytes (para formatar o valor cheque a classe {@link LocaleConverter})
   */
  public Long getSize() {
    return size;
  }

  /**
   * # tamanho do arquivo em bytes (para formatar o valor cheque a classe {@link LocaleConverter}).
   *
   * @param size the new tamanho do arquivo em bytes (para formatar o valor cheque a classe {@link LocaleConverter})
   */
  public void setSize(Long size) {
    this.size = size;
  }

  /**
   * # conteúdo do arquivo quando persistido no banco de dados {@link FilePersistenceType} = {@link FilePersistenceType#DB}.<br>
   * .
   *
   * @return the conteúdo do arquivo quando persistido no banco de dados {@link FilePersistenceType} = {@link FilePersistenceType#DB}
   */
  public FileContentVO getFileContentVO() {
    return fileContentVO;
  }

  /**
   * # conteúdo do arquivo quando persistido no banco de dados {@link FilePersistenceType} = {@link FilePersistenceType#DB}.<br>
   * .
   *
   * @param fileContentVO the new conteúdo do arquivo quando persistido no banco de dados {@link FilePersistenceType} = {@link FilePersistenceType#DB}
   */
  public void setFileContentVO(FileContentVO fileContentVO) {
    this.fileContentVO = fileContentVO;
  }

  /**
   * # encoding do conteúdo do arquivo. Geralmente utilizado para arquivos de Texto.
   *
   * @return the encoding do conteúdo do arquivo
   */
  public String getEncoding() {
    return encoding;
  }

  /**
   * # encoding do conteúdo do arquivo. Geralmente utilizado para arquivos de Texto.
   *
   * @param encoding the new encoding do conteúdo do arquivo
   */
  public void setEncoding(String encoding) {
    this.encoding = encoding;
  }

  /**
   * # local onde o arquivo é persistido.
   *
   * @return the local onde o arquivo é persistido
   */
  public FilePersistenceType getPersistenceType() {
    return persistenceType;
  }

  /**
   * # local onde o arquivo é persistido.
   *
   * @param persistenceType the new local onde o arquivo é persistido
   */
  public void setPersistenceType(FilePersistenceType persistenceType) {
    this.persistenceType = persistenceType;
  }

  /**
   * # chave de versão do arquivo quando no S3. Obrigatório quando {@link FilePersistenceType} = {@link FilePersistenceType#S3}.<br>
   * <b>ATENÇÃO:</B> Quando o FileVO tiver o conteúdo do arquivo alterado, e tivermos de postar um novo arquivo o versionID deve ser definido para nulo. o CRUD realizará um novo post no S3 quando o versionID for nulo. Caso contrário assume que o arquivo continua sendo a versão passada.
   *
   * @return the chave de versão do arquivo quando no S3
   */
  public String getVersionID() {
    return versionID;
  }

  /**
   * # chave de versão do arquivo quando no S3. Obrigatório quando {@link FilePersistenceType} = {@link FilePersistenceType#S3}.<br>
   * <b>ATENÇÃO:</B> Quando o FileVO tiver o conteúdo do arquivo alterado, e tivermos de postar um novo arquivo o versionID deve ser definido para nulo. o CRUD realizará um novo post no S3 quando o versionID for nulo. Caso contrário assume que o arquivo continua sendo a versão passada.
   *
   * @param versionID the new chave de versão do arquivo quando no S3
   */
  public void setVersionID(String versionID) {
    this.versionID = versionID;
  }

  /**
   * # tag com o nome do módulo dono do arquivo. Esta tag ajuda em caso de problemas a identificar o arquivo e encontra-lo.<br>
   * Caso seja salvo no S3, esse valor é colocado como o nome de um folder.
   *
   * @return the tag com o nome do módulo dono do arquivo
   */
  public String getTagID() {
    return tagID;
  }

  /**
   * # tag com o nome do módulo dono do arquivo. Esta tag ajuda em caso de problemas a identificar o arquivo e encontra-lo.<br>
   * Caso seja salvo no S3, esse valor é colocado como o nome de um folder.
   *
   * @param tagID the new tag com o nome do módulo dono do arquivo
   */
  public void setTagID(String tagID) {
    this.tagID = tagID;
  }

  /**
   * # caminho para o arquivo temporário onde encontramos o arquivo quando {@link FilePersistenceType} = {@link FilePersistenceType#S3}.<br>
   * Este atributo não é persistido no banco de dados pois é um caminho temporário.<br>
   * Para persistir o FileVO no S3 este atributo é obrigatório, pois o arquivo que será postado no S3 será lido desse caminho.<br>
   * Quando o FileVO for recuperado do banco de dados este atributo virá nulo. Após chamar o método de recuperação do arquivo no S3 o caminho para o arquivo baixado deve ser salvo aqui.<br>
   * Note que o Crud não verifica o conteúdo do arquivo para determinar se houve ou não alteração. Para que o CRUD saiba que o conteúdo do arquivo mudou o conteúdo de {@link #versionID} deve ser definido como null.<br>
   * O conteúdo do arquivo tempPath deve sempre acompanhar o valor definido em {@link #compression}. Ou seja, se ouver compressão o conteúdo deste arquivo já deve estar comprimido conforme a definição, se não houver, deverá ser o arquivo "pleno", e assim por diante. Assim, <b>Cuidado ao lêr o conteúdo deste arquivo</b> pois ele pode estar comprimido e precisa de tratamento antes.
   *
   * @return the caminho para o arquivo temporário onde encontramos o arquivo quando {@link FilePersistenceType} = {@link FilePersistenceType#S3}
   */
  public String getTempPath() {
    return tempPath;
  }

  /**
   * # caminho para o arquivo temporário onde encontramos o arquivo quando {@link FilePersistenceType} = {@link FilePersistenceType#S3}.<br>
   * Este atributo não é persistido no banco de dados pois é um caminho temporário.<br>
   * Para persistir o FileVO no S3 este atributo é obrigatório, pois o arquivo que será postado no S3 será lido desse caminho.<br>
   * Quando o FileVO for recuperado do banco de dados este atributo virá nulo. Após chamar o método de recuperação do arquivo no S3 o caminho para o arquivo baixado deve ser salvo aqui.<br>
   * Note que o Crud não verifica o conteúdo do arquivo para determinar se houve ou não alteração. Para que o CRUD saiba que o conteúdo do arquivo mudou o conteúdo de {@link #versionID} deve ser definido como null.<br>
   * O conteúdo do arquivo tempPath deve sempre acompanhar o valor definido em {@link #compression}. Ou seja, se ouver compressão o conteúdo deste arquivo já deve estar comprimido conforme a definição, se não houver, deverá ser o arquivo "pleno", e assim por diante. Assim, <b>Cuidado ao lêr o conteúdo deste arquivo</b> pois ele pode estar comprimido e precisa de tratamento antes.
   *
   * @param tempPath the new caminho para o arquivo temporário onde encontramos o arquivo quando {@link FilePersistenceType} = {@link FilePersistenceType#S3}
   */
  public void setTempPath(String tempPath) {
    this.tempPath = tempPath;
  }

  /**
   * # para arquivos persistidos no S3 não utilizaremos o nome do arquivo pois precisamos garantir um identificador único para que os arquivos não se sobreponham. Assim geramos um UUID que identifica o arquivo. No S3 esse UUID é utilizado como nome do arquivo.
   *
   * @return the para arquivos persistidos no S3 não utilizaremos o nome do arquivo pois precisamos garantir um identificador único para que os arquivos não se sobreponham
   */
  public String getFileUUID() {
    return fileUUID;
  }

  /**
   * # para arquivos persistidos no S3 não utilizaremos o nome do arquivo pois precisamos garantir um identificador único para que os arquivos não se sobreponham. Assim geramos um UUID que identifica o arquivo. No S3 esse UUID é utilizado como nome do arquivo.
   *
   * @param fileUUID the new para arquivos persistidos no S3 não utilizaremos o nome do arquivo pois precisamos garantir um identificador único para que os arquivos não se sobreponham
   */
  public void setFileUUID(String fileUUID) {
    this.fileUUID = fileUUID;
  }

  /**
   * # indica se o conteúdo do arquivo está comprimido ou não.<Br>
   * Quando comprimido é preciso descoprimir o conteúdo do arquivo antes de utiliza-lo.
   *
   * @return the indica se o conteúdo do arquivo está comprimido ou não
   */
  public FileCompression getCompression() {
    return compression;
  }

  /**
   * # indica se o conteúdo do arquivo está comprimido ou não.<Br>
   * Quando comprimido é preciso descoprimir o conteúdo do arquivo antes de utiliza-lo.
   *
   * @param compression the new indica se o conteúdo do arquivo está comprimido ou não
   */
  public void setCompression(FileCompression compression) {
    this.compression = compression;
  }

  /**
   * # caminho base do arquivo (pastas).<br>
   * Este caminho será utilizado ao salvar o arquivo no AWS, permitindo que o sistema separe em pastas os arquivos conforme achar conveniente. Por exemplo, por cliente, tipo de arquivo, etc.<br>
   * O caminho deve ser escrito no formato: "folder/subfolder1/../subfolderN/" SEMPRE TERMINANDO COM "/".
   *
   * @return # caminho base do arquivo (pastas)
   */
  public String getBasePath() {
    return basePath;
  }

  /**
   * # caminho base do arquivo (pastas).<br>
   * Este caminho será utilizado ao salvar o arquivo no AWS, permitindo que o sistema separe em pastas os arquivos conforme achar conveniente. Por exemplo, por cliente, tipo de arquivo, etc.<br>
   * O caminho deve ser escrito no formato: "folder/subfolder1/../subfolderN/" SEMPRE TERMINANDO COM "/".
   *
   * @param basePath # caminho base do arquivo (pastas)
   */
  public void setBasePath(String basePath) {
    this.basePath = basePath;
  }

}
