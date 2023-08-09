package br.eng.rodrigogml.rfw.files.utils;

import java.io.File;

import br.eng.rodrigogml.rfw.files.vo.FileVO;
import br.eng.rodrigogml.rfw.kernel.exceptions.RFWException;
import br.eng.rodrigogml.rfw.kernel.utils.RUFile;

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
   * Move o conteúdo do FileContentVO para um arquivo temporário e faz as alterações no FileVO.<bR>
   * Este método é útil para mover o conteúdo da memória para o arquivo temporário. Na persistência do tipo S3 o padrão é utilizar o arquivo temporário, mas para passar o conteúdo pela fachada (de sistemas externos) é necessário carregar o conteúdo no FileContentVO e Vice Versa.
   *
   * @param fileVO FileVO com o conteúdo dento do FileContentVO
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
