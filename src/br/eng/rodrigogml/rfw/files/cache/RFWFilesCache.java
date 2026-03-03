package br.eng.rodrigogml.rfw.files.cache;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.stream.Stream;
import java.util.logging.Level;
import java.util.logging.Logger;

import br.eng.rodrigogml.rfw.files.utils.RUFiles;
import br.eng.rodrigogml.rfw.files.vo.FileVO;
import br.eng.rodrigogml.rfw.kernel.exceptions.RFWException;

/**
 * Cache local para arquivos do módulo RFW.Files.
 *
 * @author Rodrigo Leitão
 */
public class RFWFilesCache {

  private static final Logger LOGGER = Logger.getLogger(RFWFilesCache.class.getName());

  private static final long DEFAULT_TTL_MINUTES = 90L;
  private static final long DEFAULT_CLEANUP_INTERVAL_MINUTES = 60L;
  private static final String DEFAULT_BASE_PATH = System.getProperty("java.io.tmpdir") + File.separator + "rfw-files-cache";
  private static final String PROP_TTL_MINUTES = "rfw.files.cache.ttl.minutes";
  private static final String PROP_CLEANUP_INTERVAL_MINUTES = "rfw.files.cache.cleanup.interval.minutes";

  private static volatile long ttlMinutes = DEFAULT_TTL_MINUTES;
  private static volatile long cleanupIntervalMinutes = DEFAULT_CLEANUP_INTERVAL_MINUTES;
  private static volatile String basePath = DEFAULT_BASE_PATH;

  private volatile File cacheBaseDir;
  private volatile String currentBasePath;
  private volatile boolean cleanupRunning;
  private volatile Thread cleanupThread;

  private RFWFilesCache() {
    loadConfigurationFromSystemProperties();
    updateCacheBaseDirIfNeeded();
    startCleanupThread();
    registerShutdownHook();
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

  public static synchronized long getCleanupIntervalMinutes() {
    return cleanupIntervalMinutes;
  }

  public static synchronized void setCleanupIntervalMinutes(long cleanupIntervalMinutes) {
    if (cleanupIntervalMinutes <= 0) {
      throw new IllegalArgumentException("cleanupIntervalMinutes deve ser maior que zero.");
    }
    RFWFilesCache.cleanupIntervalMinutes = cleanupIntervalMinutes;
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

  private void loadConfigurationFromSystemProperties() {
    setPositiveLongFromProperty(PROP_TTL_MINUTES, true);
    setPositiveLongFromProperty(PROP_CLEANUP_INTERVAL_MINUTES, false);
  }

  private void setPositiveLongFromProperty(String propertyName, boolean ttlProperty) {
    final String configuredValue = System.getProperty(propertyName);
    if (configuredValue == null || configuredValue.trim().isEmpty()) {
      return;
    }

    try {
      final long parsedValue = Long.parseLong(configuredValue.trim());
      if (parsedValue <= 0) {
        LOGGER.log(Level.WARNING, "Valor inválido para a propriedade {0}: {1}", new Object[] { propertyName, configuredValue });
        return;
      }

      if (ttlProperty) {
        setTtlMinutes(parsedValue);
      } else {
        setCleanupIntervalMinutes(parsedValue);
      }
    } catch (NumberFormatException e) {
      LOGGER.log(Level.WARNING, "Não foi possível interpretar a propriedade {0}: {1}", new Object[] { propertyName, configuredValue });
    }
  }

  private void startCleanupThread() {
    if (this.cleanupThread != null) {
      return;
    }

    this.cleanupRunning = true;
    this.cleanupThread = new Thread(() -> {
      while (this.cleanupRunning) {
        try {
          runCleanupCycle();
        } catch (Throwable t) {
          LOGGER.log(Level.WARNING, "Falha inesperada na rotina de limpeza de cache.", t);
        }

        try {
          Thread.sleep(getCleanupIntervalMinutes() * 60_000L);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          break;
        }
      }
    }, "rfw-files-cache-cleanup");

    this.cleanupThread.setDaemon(true);
    this.cleanupThread.start();
  }

  private void registerShutdownHook() {
    try {
      Runtime.getRuntime().addShutdownHook(new Thread(this::stopCleanupThread, "rfw-files-cache-cleanup-shutdown"));
    } catch (Throwable t) {
      LOGGER.log(Level.FINE, "Não foi possível registrar shutdown hook para o cache.", t);
    }
  }

  private void stopCleanupThread() {
    this.cleanupRunning = false;
    final Thread localCleanupThread = this.cleanupThread;
    if (localCleanupThread == null) {
      return;
    }

    localCleanupThread.interrupt();
    if (Thread.currentThread() == localCleanupThread) {
      return;
    }

    try {
      localCleanupThread.join(2_000L);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  private void runCleanupCycle() {
    final File localBaseDir = updateCacheBaseDirIfNeeded();
    if (!localBaseDir.exists() || !localBaseDir.isDirectory()) {
      return;
    }

    int removedFiles = 0;
    try (Stream<Path> stream = Files.walk(localBaseDir.toPath())) {
      for (Path path : (Iterable<Path>) stream::iterator) {
        final File file = path.toFile();
        if (!file.isFile()) {
          continue;
        }

        if (!isExpired(file)) {
          continue;
        }

        try {
          if (isFilePossiblyInUse(file)) {
            continue;
          }

          if (file.delete()) {
            removedFiles++;
          }
        } catch (IOException e) {
          LOGGER.log(Level.WARNING, "Erro de I/O ao remover arquivo de cache: " + file.getAbsolutePath(), e);
        }
      }
    } catch (IOException e) {
      LOGGER.log(Level.WARNING, "Erro de I/O ao varrer diretório de cache: " + localBaseDir.getAbsolutePath(), e);
    }

    if (removedFiles > 0) {
      LOGGER.log(Level.INFO, "Limpeza do cache removeu {0} arquivo(s).", removedFiles);
    }
  }

  private boolean isFilePossiblyInUse(File file) throws IOException {
    final File probe = new File(file.getParentFile(), file.getName() + ".cleanup-probe-" + System.nanoTime());
    if (!file.renameTo(probe)) {
      return true;
    }

    boolean restored = probe.renameTo(file);
    if (!restored) {
      try {
        Files.move(probe.toPath(), file.toPath(), StandardCopyOption.REPLACE_EXISTING);
        restored = true;
      } catch (IOException e) {
        LOGGER.log(Level.WARNING, "Falha ao restaurar arquivo após checagem de lock: " + file.getAbsolutePath(), e);
      }
    }

    return !restored;
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
