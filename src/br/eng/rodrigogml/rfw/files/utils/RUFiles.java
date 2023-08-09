package br.eng.rodrigogml.rfw.files.utils;

import java.io.File;

import br.eng.rodrigogml.rfw.files.vo.FileVO;
import br.eng.rodrigogml.rfw.kernel.exceptions.RFWException;
import br.eng.rodrigogml.rfw.kernel.utils.RUFile;

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
   * Move o conte�do do FileContentVO para um arquivo tempor�rio e faz as altera��es no FileVO.<bR>
   * Este m�todo � �til para mover o conte�do da mem�ria para o arquivo tempor�rio. Na persist�ncia do tipo S3 o padr�o � utilizar o arquivo tempor�rio, mas para passar o conte�do pela fachada (de sistemas externos) � necess�rio carregar o conte�do no FileContentVO e Vice Versa.
   *
   * @param fileVO FileVO com o conte�do dento do FileContentVO
   * @throws RFWException
   */
  public static void moveFileContentVOToTemporaryFile(FileVO fileVO) throws RFWException {
    if (fileVO.getTempPath() == null && fileVO.getFileContentVO() != null && fileVO.getFileContentVO().getContent() != null && fileVO.getFileContentVO().getContent().length > 0) {
      File file = RUFile.writeFileContentInTemporaryPathWithDelete(fileVO.getName(), fileVO.getFileContentVO().getContent(), 600000); // Exclui em 10 minutos
      fileVO.setTempPath(file.getAbsolutePath());
      fileVO.setFileContentVO(null);
    }
  }

}
