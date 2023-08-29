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
 * Description: Classe utilit�ria do m�dulo RFW.Files.<br>
 *
 * @author Rodrigo GML
 * @since 10.0.0 (8 de ago. de 2023)
 * @version 10.0.0 - Rodrigo GML-(...)
 */
public class RUFiles {

  /**
   * Construtor privado para classe exclusivamente est�tica.
   */
  private RUFiles() {
  }

  /**
   * Move o conte�do do {@link FileContentVO} para um arquivo tempor�rio e faz as altera��es no {@link FileVO}.<bR>
   * Este m�todo � �til para mover o conte�do da mem�ria para o arquivo tempor�rio. Na persist�ncia do tipo S3 o padr�o � utilizar o arquivo tempor�rio, mas para passar o conte�do pela fachada (de sistemas externos) � necess�rio carregar o conte�do no {@link FileContentVO} e Vice Versa.
   *
   * @param fileVO FileVO com o conte�do dento do {@link FileContentVO}
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
   * Move o conte�do do arquivo tempor�rio para o {@link FileContentVO} e faz as altera��es no {@link FileVO}.<bR>
   * Este m�todo � �til para mover o conte�do do arquivo para dentro do VO. O padr�o de manter o arquivo no tipo de persist�ncia S3 � em arquivo tempor�rio, por�m em algumas situa��es o conte�do precisa ir dentro do VO (como para cruzar a fachada), nestes caso este m�todo ajuda.
   *
   * @param fileVO FileVO com o conte�do dento do FileContentVO
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
   * Cria um {@link FileVO} baseado em um conte�do String. Utiliza o conte�do da String para criar um arquivo de texto.<Br>
   * O conte�do ser� passado para bytes com o {@link StandardCharsets#UTF_8}
   *
   * @param persistenceType Tipo de persist�ncia do arquivo, indicando onde ser� salvo.
   * @param content Conte�do bin�rio do arquivo
   * @param name nome do arquivo
   * @param tagID TagID do FileVO. Leia melhor em {@link FileVO#tagID}
   * @param compression define o modelo de compress�o a ser aplicado no conte�do do arquivo. Leia sobre os modelos em {@link FileCompression}.<br>
   *          Se for passado nulo neste atributo, o M�todo verificar� o tipo de arquivo e decidir� o modelo de compess�o sozinho. Pode demorar mais e aumentar o processamento a escolha autom�tica.
   * @return Retorna um objeto que estar� pronto para ser persistido se {@link FilePersistenceType} = {@link FilePersistenceType#DB}.<br>
   *         Se for do tipo {@link FilePersistenceType#S3} o FileVO ainda precisa ser postado no S3 para ganhar os atributos de controle do arquivo.
   * @throws RFWException
   */
  public static FileVO createFileVOFromStringUTF8(FilePersistenceType persistenceType, String content, String name, String tagID, FileCompression compression) throws RFWException {
    return createFileVO(persistenceType, content.getBytes(StandardCharsets.UTF_8), StandardCharsets.UTF_8.name(), name, tagID, compression);
  }

  /**
   * Cria um {@link FileVO} para representar o arquivo.<br>
   * Por padr�o o m�todo {@link #updateFileVO(FileVO, byte[], String)} (utilizado por este m�todo), quando o modo de persist�ncia � DB o conte�do do arquivo � colocado dentro do FileContentVO, caso seja S3 o conte�do � escrito em um arquivo tempor�rio, e n�o carregado dentro do VO.<br>
   * Caso deseje que o conte�do fique dentro do VO (para passar por fachada, por exemplo) utilize o m�todo {@link #moveTemporaryFileToFileContentVO(FileVO)} .
   *
   * @param persistenceType Tipo de persist�ncia do arquivo, indicando onde ser� salvo.
   * @param content Conte�do bin�rio do arquivo. Este conte�do ainda ser� comprimido dependendo do modelo de {@link FileCompression} passado.<br>
   *          Note que se j� se tratar de um arquivo comprimido, ou mesmo algum tipo de arquivo bin�rio como Imagens em geral, o arquivo poder� ficar maior que o normal.
   * @param encoding Encoding dos caracteres quando trata-se de um arquivo de texto. Essa informa��o � simplesmente salva no VO para orientar a leitura do arquivo. Nenhum processamento � realizado aqui.
   * @param name nome do arquivo
   * @param tagID TagID do FileVO. Leia melhor em {@link FileVO#tagID}
   * @param compression define o modelo de compress�o a ser aplicado no conte�do do arquivo. Leia sobre os modelos em {@link FileCompression}.<br>
   *          Se for passado nulo neste atributo, o M�todo verificar� o tipo de arquivo e decidir� o modelo de compess�o sozinho. Pode demorar mais e aumentar o processamento a escolha autom�tica.
   * @return Retorna um objeto que estar� pronto para ser persistido se {@link FilePersistenceType} = {@link FilePersistenceType#DB}.<br>
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
   * M�todo auxiliar para processar e escrever o cont�udo no {@link FileVO} para ser persistido.
   *
   * @param fileVO Objeto a ser processado e escrito.
   * @param content Conte�do do arquivo.
   * @param encoding Encoding do Conte�do do FileVO.
   * @return
   * @throws RFWException
   */
  public static FileVO updateFileVO(FileVO fileVO, byte[] content, String encoding) throws RFWException {
    PreProcess.requiredNonNullCritical(fileVO, "FileVO n�o pode ser nulo!");
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
            // Mantemos o arquivo compactado que j� criamos no in�cio para compara��o.
            break;
          case NONE:
            // Se n�o recebemos comando de compacta��o, criamos um arquivo tempor�rio com o conte�do original sem compacta��o
            zipFile = RUFile.createFileInTemporaryPathWithDelete(fileVO.getName(), new String(content, Charset.forName(encoding)), Charset.forName(encoding), -1);
            break;
        }
        fileVO.setSize(zipFile.length());
        fileVO.setTempPath(zipFile.getAbsolutePath());
        fileVO.setFileUUID(RUGenerators.generateUUID());
        fileVO.setVersionID(null); // Garante que o FileVO ser� processado e persistido pelo Core
        break;
    }
    return fileVO;
  }

  /**
   * Este m�todo ajuda a extrair o arquivo correto do FileVO.<br>
   * Considerando {@link FileVO} com o tipo de persist�ncia do tipo {@link FilePersistenceType#S3}, o arquivo apontado por {@link FileVO#getTempPath()} pode ser um arquivo compactado de alguma forma, conforme definido no {@link FileVO#getCompression()}. Este m�todo verifica todas essas condi��es e, se necess�rio descompacta e deixa o arquivo no formato correto novamente, retornando o caminho de
   * onde encontrar o arquivo pronto.
   *
   * @param vo FileVO para l�r as condi��es e encontrar o arquivo tempor�rio. Deve vir com o {@link FileVO#getTempPath()}} j� preenchido
   * @return Caminho para o arquivo pronto para uso. Em algumas situa��es, pode retornar o mesmo Path do VO se n�o houver nenhum processamento apra ser realizado.
   * @throws RFWException
   */
  public static String processFileVOTempFile(FileVO vo) throws RFWException {
    if (vo.getPersistenceType() != FilePersistenceType.S3) throw new RFWCriticalException("Este m�todo s� pode ser utilizado com Objetos persistidos no S3!");
    if (vo.getTempPath() == null) throw new RFWCriticalException("� esperado que o FileVO j� tenha com o arquivo tempor�rio definido!");

    switch (vo.getCompression()) {
      case MAXIMUM_COMPRESSION:
        try {
          final File file = RUFile.createFileInTemporaryPath(vo.getName());
          RUZip.extractZipEntry(new FileInputStream(vo.getTempPath()), vo.getName(), file);
          return file.getAbsolutePath();
        } catch (FileNotFoundException e) {
          throw new RFWCriticalException("Falha ao l�r o arquivo tempor�rio para obter o conte�do do FilVO!", e);
        }
      case NONE:
        return vo.getTempPath();
    }

    return null;
  }
}
