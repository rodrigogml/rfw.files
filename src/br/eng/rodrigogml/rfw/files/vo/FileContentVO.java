package br.eng.rodrigogml.rfw.files.vo;

import java.io.Serializable;

import br.eng.rodrigogml.rfw.kernel.rfwmeta.RFWMetaByteArrayField;
import br.eng.rodrigogml.rfw.kernel.rfwmeta.RFWMetaRelationshipField;
import br.eng.rodrigogml.rfw.kernel.rfwmeta.RFWMetaRelationshipField.RelationshipTypes;
import br.eng.rodrigogml.rfw.kernel.vo.RFWVO;

/**
 * Description: Classe usada para guardar o conteúdo do arquivo separado das outras informações.<br>
 *
 * @author Rodrigo Leitão
 * @since 3.2.0 (DEZ / 2009)
 */
public class FileContentVO extends RFWVO implements Serializable {

  private static final long serialVersionUID = 3302186106595703387L;

  /**
   * Definições do Arquivo.
   */
  @RFWMetaRelationshipField(caption = "Arquivo", required = true, relationship = RelationshipTypes.PARENT_ASSOCIATION)
  private FileVO fileVO = null;

  /**
   * Conteúdo do arquivo.
   */
  @RFWMetaByteArrayField(caption = "Conteúdo", required = true)
  private byte[] content = null;

  /**
   * Recupera o conteúdo do arquivo.
   *
   * @return the conteúdo do arquivo
   */
  public byte[] getContent() {
    return content;
  }

  /**
   * Define o conteúdo do arquivo.
   *
   * @param content the new conteúdo do arquivo
   */
  public void setContent(byte[] content) {
    this.content = content;
  }

  /**
   * Recupera o definições do Arquivo.
   *
   * @return the definições do Arquivo
   */
  public FileVO getFileVO() {
    return fileVO;
  }

  /**
   * Define o definições do Arquivo.
   *
   * @param fileVO the new definições do Arquivo
   */
  public void setFileVO(FileVO fileVO) {
    this.fileVO = fileVO;
  }

}
