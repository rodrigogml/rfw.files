package br.eng.rodrigogml.rfw.files.cache;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import br.eng.rodrigogml.rfw.files.vo.FileVO;

/**
 * Cache local para arquivos do módulo RFW.Files.
 *
 * @author Rodrigo Leitão
 */
public class RFWFilesCache {

  private static final long DEFAULT_TTL_MINUTES = 90L;
  private static final String DEFAULT_BASE_PATH = System.getProperty("java.io.tmpdir") + File.separator + "rfw-files-cache";

  private static volatile long ttlMinutes = DEFAULT_TTL_MINUTES;
  private static volatile String basePath = DEFAULT_BASE_PATH;

  private volatile File cacheBaseDir;
  private volatile String currentBasePath;

  private RFWFilesCache() {
    updateCacheBaseDirIfNeeded();
  }

  private static class Holder {
    private static final RFWFilesCache INSTANCE = new RFWFilesCache();
  }

  public static RFWFilesCache getInstance() {
    return Holder.INSTANCE;
  }

  public static synchronized long getTtlMinutes() {
    return ttlMinutes;
  }

  public static synchronized void setTtlMinutes(long ttlMinutes) {
    if (ttlMinutes <= 0) {
      throw new IllegalArgumentException("ttlMinutes deve ser maior que zero.");
    }
    RFWFilesCache.ttlMinutes = ttlMinutes;
  }

  public static synchronized String getBasePath() {
    return basePath;
  }

  public static synchronized void setBasePath(String basePath) {
    if (basePath == null || basePath.trim().isEmpty()) {
      throw new IllegalArgumentException("basePath não pode ser nulo ou vazio.");
    }
    RFWFilesCache.basePath = basePath;
  }

  public File get(FileVO vo) {
    final File cachedFile = resolveCacheFile(vo);
    if (!cachedFile.exists()) {
      return null;
    }

    if (isExpired(cachedFile)) {
      cachedFile.delete();
      return null;
    }

    touch(cachedFile);
    return cachedFile;
  }

  public File put(FileVO vo, File sourceFile) {
    if (sourceFile == null || !sourceFile.exists() || !sourceFile.isFile()) {
      throw new IllegalArgumentException("sourceFile inválido para cache.");
    }

    final File cachedFile = resolveCacheFile(vo);
    final File parent = cachedFile.getParentFile();
    if (!parent.exists()) {
      parent.mkdirs();
    }

    try {
      Files.copy(sourceFile.toPath(), cachedFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
    } catch (IOException e) {
      throw new RuntimeException("Falha ao copiar arquivo para o cache: " + cachedFile.getAbsolutePath(), e);
    }

    touch(cachedFile);
    return cachedFile;
  }

  public void invalidate(FileVO vo) {
    final File cachedFile = resolveCacheFile(vo);
    if (cachedFile.exists()) {
      cachedFile.delete();
    }
  }

  public void touch(File cachedFile) {
    if (cachedFile != null && cachedFile.exists()) {
      cachedFile.setLastModified(System.currentTimeMillis());
    }
  }

  private boolean isExpired(File cachedFile) {
    final long ttlMillis = getTtlMinutes() * 60_000L;
    return System.currentTimeMillis() - cachedFile.lastModified() > ttlMillis;
  }

  private File resolveCacheFile(FileVO vo) {
    if (vo == null) {
      throw new IllegalArgumentException("vo não pode ser nulo.");
    }

    final String digest = buildDigest(vo);
    final String extension = extractExtension(vo.getName());
    final String relativePath = digest.substring(0, 2) + File.separator + digest + extension;

    return new File(updateCacheBaseDirIfNeeded(), relativePath);
  }

  private synchronized File updateCacheBaseDirIfNeeded() {
    final String configuredBasePath = getBasePath();
    if (this.cacheBaseDir == null || !configuredBasePath.equals(this.currentBasePath)) {
      this.currentBasePath = configuredBasePath;
      this.cacheBaseDir = new File(configuredBasePath);
      if (!this.cacheBaseDir.exists()) {
        this.cacheBaseDir.mkdirs();
      }
    }
    return this.cacheBaseDir;
  }

  private String buildDigest(FileVO vo) {
    final String rawKey = String.valueOf(vo.getFileUUID())
        + "|" + String.valueOf(vo.getVersionID())
        + "|" + String.valueOf(vo.getTagID())
        + "|" + String.valueOf(vo.getBasePath())
        + "|" + String.valueOf(vo.getName());

    try {
      final MessageDigest md = MessageDigest.getInstance("SHA-256");
      final byte[] digest = md.digest(rawKey.getBytes(StandardCharsets.UTF_8));
      final StringBuilder builder = new StringBuilder();
      for (byte b : digest) {
        builder.append(String.format("%02x", b));
      }
      return builder.toString();
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException("SHA-256 não disponível para gerar chave do cache.", e);
    }
  }

  private String extractExtension(String fileName) {
    if (fileName == null) {
      return "";
    }

    final int dotIndex = fileName.lastIndexOf('.');
    if (dotIndex <= 0 || dotIndex == fileName.length() - 1) {
      return "";
    }

    return fileName.substring(dotIndex);
  }
}
