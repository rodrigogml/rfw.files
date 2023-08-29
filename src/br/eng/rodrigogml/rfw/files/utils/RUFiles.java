package br.eng.rodrigogml.rfw.files.utils;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;

import br.eng.rodrigogml.rfw.files.vo.FileContentVO;
import br.eng.rodrigogml.rfw.files.vo.FileVO;
import br.eng.rodrigogml.rfw.files.vo.FileVO.FileCompression;
import br.eng.rodrigogml.rfw.files.vo.FileVO.FilePersistenceType;
import br.eng.rodrigogml.rfw.kernel.RFW;
import br.eng.rodrigogml.rfw.kernel.exceptions.RFWCriticalException;
import br.eng.rodrigogml.rfw.kernel.exceptions.RFWException;
import br.eng.rodrigogml.rfw.kernel.preprocess.PreProcess;
import br.eng.rodrigogml.rfw.kernel.utils.RUFile;
import br.eng.rodrigogml.rfw.kernel.utils.RUGenerators;
import br.eng.rodrigogml.rfw.kernel.utils.RUZip;

/**
 * Description: Classe utilitária do módulo RFW.Files.<br>
 *
 * @author Rodrigo GML
 * @since 10.0.0 (8 de ago. de 2023)
 * @version 10.0.0 - Rodrigo GML-(...)
 */
public class RUFiles {

  /**
   * Construtor privado para classe exclusivamente estática.
   */
  private RUFiles() {
  }

  /**
   * Move o conteúdo do {@link FileContentVO} para um arquivo temporário e faz as alterações no {@link FileVO}.<bR>
   * Este método é útil para mover o conteúdo da memória para o arquivo temporário. Na persistência do tipo S3 o padrão é utilizar o arquivo temporário, mas para passar o conteúdo pela fachada (de sistemas externos) é necessário carregar o conteúdo no {@link FileContentVO} e Vice Versa.
   *
   * @param fileVO FileVO com o conteúdo dento do {@link FileContentVO}
   * @throws RFWException
   */
  public static void moveFileContentVOToTemporaryFile(FileVO fileVO) throws RFWException {
    if (fileVO.getTempPath() == null && fileVO.getFileContentVO() != null && fileVO.getFileContentVO().getContent() != null && fileVO.getFileContentVO().getContent().length > 0) {
      File file = RUFile.writeFileContentInTemporaryPathWithDelete(fileVO.getName(), fileVO.getFileContentVO().getContent(), 600000); // Exclui em 10 minutos
      fileVO.setTempPath(file.getAbsolutePath());
      fileVO.setFileContentVO(null);
    }
  }

  /**
   * Move o conteúdo do arquivo temporário para o {@link FileContentVO} e faz as alterações no {@link FileVO}.<bR>
   * Este método é útil para mover o conteúdo do arquivo para dentro do VO. O padrão de manter o arquivo no tipo de persistência S3 é em arquivo temporário, porém em algumas situações o conteúdo precisa ir dentro do VO (como para cruzar a fachada), nestes caso este método ajuda.
   *
   * @param fileVO FileVO com o conteúdo dento do FileContentVO
   * @throws RFWException
   */
  public static void moveTemporaryFileToFileContentVO(FileVO fileVO) throws RFWException {
    if (fileVO.getTempPath() != null && fileVO.getFileContentVO() == null) {
      FileContentVO fcVO = new FileContentVO();
      fcVO.setFileVO(fileVO);
      fcVO.setContent(RUFile.readFileContent(fileVO.getTempPath()));
      fileVO.setFileContentVO(fcVO);
      fileVO.setTempPath(null);
    }
  }

  /**
   * Cria um {@link FileVO} baseado em um conteúdo String. Utiliza o conteúdo da String para criar um arquivo de texto.<Br>
   * O conteúdo será passado para bytes com o {@link StandardCharsets#UTF_8}
   *
   * @param persistenceType Tipo de persistência do arquivo, indicando onde será salvo.
   * @param content Conteúdo binário do arquivo
   * @param name nome do arquivo
   * @param tagID TagID do FileVO. Leia melhor em {@link FileVO#tagID}
   * @param compression define o modelo de compressão a ser aplicado no conteúdo do arquivo. Leia sobre os modelos em {@link FileCompression}.<br>
   *          Se for passado nulo neste atributo, o Método verificará o tipo de arquivo e decidirá o modelo de compessão sozinho. Pode demorar mais e aumentar o processamento a escolha automática.
   * @return Retorna um objeto que estará pronto para ser persistido se {@link FilePersistenceType} = {@link FilePersistenceType#DB}.<br>
   *         Se for do tipo {@link FilePersistenceType#S3} o FileVO ainda precisa ser postado no S3 para ganhar os atributos de controle do arquivo.
   * @throws RFWException
   */
  public static FileVO createFileVOFromStringUTF8(FilePersistenceType persistenceType, String content, String name, String tagID, FileCompression compression) throws RFWException {
    return createFileVO(persistenceType, content.getBytes(StandardCharsets.UTF_8), StandardCharsets.UTF_8.name(), name, tagID, compression);
  }

  /**
   * Cria um {@link FileVO} para representar o arquivo.<br>
   * Por padrão o método {@link #updateFileVO(FileVO, byte[], String)} (utilizado por este método), quando o modo de persistência é DB o conteúdo do arquivo é colocado dentro do FileContentVO, caso seja S3 o conteúdo é escrito em um arquivo temporário, e não carregado dentro do VO.<br>
   * Caso deseje que o conteúdo fique dentro do VO (para passar por fachada, por exemplo) utilize o método {@link #moveTemporaryFileToFileContentVO(FileVO)} .
   *
   * @param persistenceType Tipo de persistência do arquivo, indicando onde será salvo.
   * @param content Conteúdo binário do arquivo. Este conteúdo ainda será comprimido dependendo do modelo de {@link FileCompression} passado.<br>
   *          Note que se já se tratar de um arquivo comprimido, ou mesmo algum tipo de arquivo binário como Imagens em geral, o arquivo poderá ficar maior que o normal.
   * @param encoding Encoding dos caracteres quando trata-se de um arquivo de texto. Essa informação é simplesmente salva no VO para orientar a leitura do arquivo. Nenhum processamento é realizado aqui.
   * @param name nome do arquivo
   * @param tagID TagID do FileVO. Leia melhor em {@link FileVO#tagID}
   * @param compression define o modelo de compressão a ser aplicado no conteúdo do arquivo. Leia sobre os modelos em {@link FileCompression}.<br>
   *          Se for passado nulo neste atributo, o Método verificará o tipo de arquivo e decidirá o modelo de compessão sozinho. Pode demorar mais e aumentar o processamento a escolha automática.
   * @return Retorna um objeto que estará pronto para ser persistido se {@link FilePersistenceType} = {@link FilePersistenceType#DB}.<br>
   *         Se for do tipo {@link FilePersistenceType#S3} o FileVO ainda precisa ser postado no S3 para ganhar os atributos de controle do arquivo.
   * @throws RFWException
   */
  public static FileVO createFileVO(FilePersistenceType persistenceType, byte[] content, String encoding, String name, String tagID, FileCompression compression) throws RFWException {
    final LocalDateTime now = RFW.getDateTime();

    final FileVO fileVO = new FileVO();
    fileVO.setDateCreation(now);
    fileVO.setTagID(tagID);
    fileVO.setName(name);
    fileVO.setPersistenceType(persistenceType);
    fileVO.setCompression(compression);

    return updateFileVO(fileVO, content, encoding);
  }

  /**
   * Método auxiliar para processar e escrever o contéudo no {@link FileVO} para ser persistido.
   *
   * @param fileVO Objeto a ser processado e escrito.
   * @param content Conteúdo do arquivo.
   * @param encoding Encoding do Conteúdo do FileVO.
   * @return
   * @throws RFWException
   */
  public static FileVO updateFileVO(FileVO fileVO, byte[] content, String encoding) throws RFWException {
    PreProcess.requiredNonNullCritical(fileVO, "FileVO não pode ser nulo!");
    PreProcess.requiredNonNullCritical(fileVO.getPersistenceType(), "FileVO precisa ter o atributo 'persistenceType' definido!");

    final LocalDateTime now = RFW.getDateTime();
    fileVO.setDateModification(now);
    fileVO.setEncoding(encoding);

    FileCompression compression = fileVO.getCompression();

    File zipFile = null;
    if (compression == null || compression == FileCompression.MAXIMUM_COMPRESSION) {
      String zipName = RUFile.extractFileName(fileVO.getName()) + ".zip";
      String zipFilePath = RUZip.createNewZipFile(zipName, 9, new Object[][] {
          { fileVO.getName(), new ByteArrayInputStream(content) }
      });
      zipFile = new File(zipFilePath);

      if (compression == null) { // Se compression for nulo escolhemos conforme a economia de tamanho
        if (zipFile.length() < content.length) {
          compression = FileCompression.MAXIMUM_COMPRESSION;
        } else {
          compression = FileCompression.NONE;
        }
        fileVO.setCompression(compression);
      }
    }

    switch (fileVO.getPersistenceType()) {
      case DB:
        final FileContentVO contentVO = new FileContentVO();
        fileVO.setFileContentVO(contentVO);
        contentVO.setFileVO(fileVO);

        switch (compression) {
          case MAXIMUM_COMPRESSION:
            content = RUFile.readFileContent(zipFile);
            break;
          case NONE:
            break;
        }
        fileVO.setSize((long) content.length);
        contentVO.setContent(content);
        break;
      case S3:
        switch (compression) {
          case MAXIMUM_COMPRESSION:
            // Mantemos o arquivo compactado que já criamos no início para comparação.
            break;
          case NONE:
            // Se não recebemos comando de compactação, criamos um arquivo temporário com o conteúdo original sem compactação
            zipFile = RUFile.createFileInTemporaryPathWithDelete(fileVO.getName(), new String(content, Charset.forName(encoding)), Charset.forName(encoding), -1);
            break;
        }
        fileVO.setSize(zipFile.length());
        fileVO.setTempPath(zipFile.getAbsolutePath());
        fileVO.setFileUUID(RUGenerators.generateUUID());
        fileVO.setVersionID(null); // Garante que o FileVO será processado e persistido pelo Core
        break;
    }
    return fileVO;
  }

  /**
   * Este método ajuda a extrair o arquivo correto do FileVO.<br>
   * Considerando {@link FileVO} com o tipo de persistência do tipo {@link FilePersistenceType#S3}, o arquivo apontado por {@link FileVO#getTempPath()} pode ser um arquivo compactado de alguma forma, conforme definido no {@link FileVO#getCompression()}. Este método verifica todas essas condições e, se necessário descompacta e deixa o arquivo no formato correto novamente, retornando o caminho de
   * onde encontrar o arquivo pronto.
   *
   * @param vo FileVO para lêr as condições e encontrar o arquivo temporário. Deve vir com o {@link FileVO#getTempPath()}} já preenchido
   * @return Caminho para o arquivo pronto para uso. Em algumas situações, pode retornar o mesmo Path do VO se não houver nenhum processamento apra ser realizado.
   * @throws RFWException
   */
  public static String processFileVOTempFile(FileVO vo) throws RFWException {
    if (vo.getPersistenceType() != FilePersistenceType.S3) throw new RFWCriticalException("Este método só pode ser utilizado com Objetos persistidos no S3!");
    if (vo.getTempPath() == null) throw new RFWCriticalException("É esperado que o FileVO já tenha com o arquivo temporário definido!");

    switch (vo.getCompression()) {
      case MAXIMUM_COMPRESSION:
        try {
          final File file = RUFile.createFileInTemporaryPath(vo.getName());
          RUZip.extractZipEntry(new FileInputStream(vo.getTempPath()), vo.getName(), file);
          return file.getAbsolutePath();
        } catch (FileNotFoundException e) {
          throw new RFWCriticalException("Falha ao lêr o arquivo temporário para obter o conteúdo do FilVO!", e);
        }
      case NONE:
        return vo.getTempPath();
    }

    return null;
  }
}
