package br.eng.rodrigogml.rfw.files.crud;

import java.io.File;
import java.util.HashMap;

import br.eng.rodrigogml.rfw.files.aws.RFWS3;
import br.eng.rodrigogml.rfw.files.utils.RUFiles;
import br.eng.rodrigogml.rfw.files.vo.FileContentVO;
import br.eng.rodrigogml.rfw.files.vo.FileContentVO_;
import br.eng.rodrigogml.rfw.files.vo.FileVO;
import br.eng.rodrigogml.rfw.files.vo.FileVO.FileCompression;
import br.eng.rodrigogml.rfw.files.vo.FileVO.FilePersistenceType;
import br.eng.rodrigogml.rfw.kernel.exceptions.RFWCriticalException;
import br.eng.rodrigogml.rfw.kernel.exceptions.RFWException;
import br.eng.rodrigogml.rfw.kernel.exceptions.RFWValidationException;
import br.eng.rodrigogml.rfw.kernel.interfaces.RFWDBProvider;
import br.eng.rodrigogml.rfw.kernel.preprocess.PreProcess;
import br.eng.rodrigogml.rfw.kernel.utils.RUFile;
import br.eng.rodrigogml.rfw.kernel.validator.RFWValidator;
import br.eng.rodrigogml.rfw.kernel.vo.RFWMO;
import br.eng.rodrigogml.rfw.orm.dao.RFWDAO;

import software.amazon.awssdk.services.s3.model.PutObjectResponse;

/**
 * Description: Classe de manipulação dos objetos {@link FileVO}.<br>
 *
 * @author Rodrigo GML
 * @since 1.0.0 (8 de ago. de 2023)
 * @version 1.0.0 - Rodrigo GML-(...)
 */
public class FilesCrud {

  private FilesCrud() {
  }

  /**
   * Persiste um {@link FileVO} no banco de dados, e realizar o post no S3.<br>
   * <b>Note</b> que o posto no S3 só é realizado se detectar que houve uma mudança no arquivo. Onde "mudança no arquivo" quer dizer que o atributo {@link FileVO#getVersionID()} está nulo. Para garantir que o arquivo será atualizado na S3 o {@link FileVO} deve vir com o valor deste atributo definido como nulo.<br>
   * <br>
   * <b>Atenção:</b> Este método não é publicado na fachada, ele deve ser chamado diretamente do crud dos objetos que utilizam o FileVO.<br>
   * <br>
   * Este método ignora o <b>fullLoaded</b> do objeto. Por ser um objeto simples e sempre realizar uma validação completa dos dados, não é necessário vir com a chancela de carregamento do Banco de dados.
   *
   * @param vo Objeto a ser persistido representando o arquivo.
   * @param validator Validador com o {@link RFWDBProvider} definido para validação no banco de dados.
   * @param daoFile DAO do {@link FileVO} para acesso direto ao banco de dados.
   * @param daoFileContent DAO do {@link FileContentVO} para acesso direto ao banco de dados. Obrigatório quando o {@link FileVO#getPersistenceType()} for do tipo {@link FilePersistenceType#DB}.
   * @param rfws3 Instância para manipulação do S3 já criado. Obrigatório quando o {@link FileVO#getPersistenceType()} for do tipo {@link FilePersistenceType#S3}.
   * @param bucket Nome do bucket do S3 para persistência. Obrigatório quando o {@link FileVO#getPersistenceType()} for do tipo {@link FilePersistenceType#S3}.
   * @return Objeto conforme foi persistido (com o ID).
   * @throws RFWException
   */
  public static FileVO persistFile(FileVO vo, RFWValidator validator, RFWDAO<FileVO> daoFile, RFWDAO<FileContentVO> daoFileContent, RFWS3 rfws3, String bucket) throws RFWException {
    PreProcess.processVO(vo);
    {// Mais preprocessamento
      if (vo.getPersistenceType() != null) {
        switch (vo.getPersistenceType()) {
          case DB:
            vo.setFileUUID(null);
            vo.setVersionID(null);
            break;
          case S3:
            // No tipo S3 o conteúdo do arquivo pode vir por arquivo temporário ou FileContentVO
            break;
        }
      }
    }

    validator.validatePersist(FileVO.class, vo);

    switch (vo.getPersistenceType()) {
      case DB: {
        if (vo.getFileContentVO() == null || vo.getFileContentVO().getContent() == null || vo.getFileContentVO().getContent().length == 0) throw new RFWValidationException("FileContentVO é obrigatório para arquivos persistidos no banco de dados.");

        // Para Garantir que não fiquem múltiplos FileContentVO para o mesmo FileVO (caso o objeto venha incompleto de ID para persistir), excluímos FileContentVO que possam estar associados à este FileVO
        if (vo.getId() != null) {
          RFWMO mo = new RFWMO();
          mo.equal(FileContentVO_.vo().fileVO().id(), vo.getId());
          if (vo.getFileContentVO().getId() != null) mo.notEqual(FileContentVO_._id, vo.getFileContentVO().getId()); // Não permite encontrar o mesmo FileContentVO
          FileContentVO oldVO = daoFileContent.findUniqueMatch(mo, null);
          if (oldVO != null) {
            daoFileContent.delete(oldVO.getId());
          }
        }
        vo = daoFile.persist(vo, true);
      }
        break;
      case S3: {
        // Se for o S3, verificamos se não temos um id de versão temos de postar o arquivo para obter uma.
        if (vo.getVersionID() == null) {
          if (vo.getFileUUID() == null) throw new RFWValidationException("FileUUID é obrigatório é obrigatório para arquivos persistidos no S3.");
          if (vo.getTempPath() == null && (vo.getFileContentVO() == null || vo.getFileContentVO().getContent() == null || vo.getFileContentVO().getContent().length == 0)) {
            throw new RFWValidationException("Não foi possível encontrar o conteúdo do arquivo para persistir no S3.");
          }

          // Para persistir no RFWS3 o conteúdo do arquivo precisa estar em um arquivo temporário (não passa por Stream), assim garantimos que o conteúdo a ser persistido está em um arquivo temporário
          RUFiles.moveFileContentVOToTemporaryFile(vo);

          if (vo.getVersionID() == null) {
            final File file = new File(vo.getTempPath());
            if (!file.exists()) throw new RFWCriticalException("O arquivo do FileVO não pode ser encontrado! Falha ao postar o arquivo.");

            final String s3Path = createS3FilePath(vo);

            PutObjectResponse result = rfws3.putObject(bucket, s3Path, file);
            vo.setVersionID(result.versionId());

            // Criamos uma lista de Tags para auxiliar a identificar os atributos quando olhados no S3.
            HashMap<String, String> tagsMap = new HashMap<String, String>();
            tagsMap.put("compression", vo.getCompression().name());
            tagsMap.put("fileName", vo.getName());

            rfws3.putObjectTags(bucket, s3Path, tagsMap);
          }

          vo = daoFile.persist(vo, true);
        }
      }
        break;
    }

    return vo;
  }

  /**
   * Monta o filePath onde o arquivo será salvo na S3.
   *
   * @param fileVO Objeto com as informações para referência.
   * @return Caminho para salvar ou recuperar o arquivo do Bucket do S3.
   * @throws RFWException
   */
  private static String createS3FilePath(FileVO fileVO) throws RFWException {
    StringBuilder buff = new StringBuilder();
    if (fileVO.getBasePath() != null) {
      if ('/' != fileVO.getBasePath().charAt(fileVO.getBasePath().length() - 1)) throw new RFWCriticalException("BasePath do FileVO deve sempre terminar com o '/'!");
      buff.append(fileVO.getBasePath());
    }
    buff.append(fileVO.getFileUUID()).append(".");
    switch (fileVO.getCompression()) {
      case MAXIMUM_COMPRESSION:
        buff.append("zip");
        break;
      case NONE:
        buff.append(RUFile.extractFileExtension(fileVO.getName()));
        break;
    }

    return buff.toString();
  }

  /**
   * Recupera um arquivo que esteja armazenado no S3 fazendo o download e salvando em um arquivo temporário no sistema.<br>
   * O método só retorna depois que o download finalizar e o arquivo estiver disponível. <Br>
   * Este método já escreve no {@link FileVO#getTempPath()} o caminho para o arquivo temporário com o conteúdo do arquivo.
   *
   * @param vo FileVO para acessar já com as informações do arquivo. Mesma função que o {@link #retrieveFileVOFromS3(Long)} mas evita a consulta no banco caso já tenhamos o objeto em memória.
   * @param rfws3 Instância para manipulação do S3 já criado. Obrigatório quando o {@link FileVO#getPersistenceType()} for do tipo {@link FilePersistenceType#S3}.
   * @param bucket Nome do bucket do S3 para persistência. Obrigatório quando o {@link FileVO#getPersistenceType()} for do tipo {@link FilePersistenceType#S3}.
   * @return O mesmo objeto recebido com o atributo {@link FileVO#getTempPath()} definido com o caminho do arquivo temporário.
   * @throws RFWException
   */
  public static FileVO retrieveFileVOFromS3(FileVO vo, RFWS3 rfws3, String bucket) throws RFWException {
    PreProcess.requiredNonNullCritical(vo, "FileVO não pode ser nulo!");
    PreProcess.requiredNonNullCritical(vo.getVersionID(), "FileVO não contem uma versionID definida!");

    String fileName = vo.getName();
    if (vo.getCompression() == FileCompression.MAXIMUM_COMPRESSION) {
      fileName = RUFile.extractFileName(fileName) + ".zip";
    }
    File file = RUFile.createFileInTemporaryPathWithDelete(fileName, -1);
    rfws3.getObject(bucket, createS3FilePath(vo), vo.getVersionID(), file);

    vo.setTempPath(file.getAbsolutePath());
    return vo;
  }
}
