package br.eng.rodrigogml.rfw.files.cache;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;

import br.eng.rodrigogml.rfw.files.utils.RUFiles;
import br.eng.rodrigogml.rfw.files.vo.FileVO;
import br.eng.rodrigogml.rfw.kernel.exceptions.RFWException;

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
    return get(vo, null);
  }

  public File get(FileVO vo, String bucket) {
    final File cachedFile = resolveCacheFile(vo, bucket);
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
    return put(vo, sourceFile, null);
  }

  public File put(FileVO vo, File sourceFile, String bucket) {
    if (sourceFile == null || !sourceFile.exists() || !sourceFile.isFile()) {
      throw new IllegalArgumentException("sourceFile inválido para cache.");
    }

    final File cachedFile = resolveCacheFile(vo, bucket);
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
    invalidate(vo, null);
  }

  public void invalidate(FileVO vo, String bucket) {
    final File cachedFile = resolveCacheFile(vo, bucket);
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

  private File resolveCacheFile(FileVO vo, String bucket) {
    if (vo == null) {
      throw new IllegalArgumentException("vo não pode ser nulo.");
    }

    final String relativePath;
    try {
      relativePath = buildRelativeCachePath(vo, bucket);
    } catch (RFWException e) {
      throw new RuntimeException("Falha ao resolver caminho do cache para o FileVO.", e);
    }

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

  private String buildRelativeCachePath(FileVO vo, String bucket) throws RFWException {
    final StringBuilder relativePath = new StringBuilder();

    final String bucketSegment = sanitizePathSegment(bucket);
    if (!bucketSegment.isEmpty()) {
      relativePath.append(bucketSegment).append(File.separator);
    }

    final String s3Path = RUFiles.createS3FilePath(vo);
    final int lastSlash = s3Path.lastIndexOf('/');
    if (lastSlash > 0) {
      final String normalizedBasePath = normalizePath(s3Path.substring(0, lastSlash));
      if (!normalizedBasePath.isEmpty()) {
        relativePath.append(normalizedBasePath).append(File.separator);
      }
    }

    relativePath.append(buildCacheFileName(vo));
    return relativePath.toString();
  }

  private String buildCacheFileName(FileVO vo) {
    final String extension = extractExtension(vo.getName());
    final String sanitizedVersionID = sanitizeFileNameSegment(vo.getVersionID());
    return String.valueOf(vo.getFileUUID()) + "__v_" + sanitizedVersionID + extension;
  }

  private String normalizePath(String path) {
    if (path == null) {
      return "";
    }

    String normalized = path.trim();
    while (normalized.startsWith("/")) {
      normalized = normalized.substring(1);
    }
    while (normalized.endsWith("/")) {
      normalized = normalized.substring(0, normalized.length() - 1);
    }

    StringBuilder safePath = new StringBuilder();
    String[] segments = normalized.split("/");
    for (String segment : segments) {
      final String sanitized = sanitizePathSegment(segment);
      if (!sanitized.isEmpty()) {
        if (safePath.length() > 0) {
          safePath.append(File.separator);
        }
        safePath.append(sanitized);
      }
    }
    return safePath.toString();
  }

  private String sanitizePathSegment(String segment) {
    if (segment == null) {
      return "";
    }
    return segment.trim().replaceAll("[^a-zA-Z0-9._-]", "_");
  }

  private String sanitizeFileNameSegment(String segment) {
    final String sanitized = sanitizePathSegment(segment);
    if (sanitized.isEmpty()) {
      return "no-version";
    }
    return sanitized;
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
