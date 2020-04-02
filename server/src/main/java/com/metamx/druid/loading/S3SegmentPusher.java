package com.metamx.druid.loading;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Closeables;
import com.metamx.common.ISE;
import com.metamx.common.StreamUtils;
import com.metamx.druid.client.DataSegment;
import com.metamx.druid.index.v1.IndexIO;
import com.metamx.emitter.EmittingLogger;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.codehaus.jackson.map.ObjectMapper;
import org.jets3t.service.S3ServiceException;
import org.jets3t.service.impl.rest.httpclient.RestS3Service;
import org.jets3t.service.model.S3Object;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
public class S3SegmentPusher implements SegmentPusher {
  public static EmittingLogger log=new EmittingLogger(S3SegmentPusher.class);
  public static Joiner JOINER=Joiner.on("/").skipNulls();
  public RestS3Service s3Client;
  public S3SegmentPusherConfig config;
  public ObjectMapper jsonMapper;
  public S3SegmentPusher(  RestS3Service s3Client,  S3SegmentPusherConfig config,  ObjectMapper jsonMapper){
    this.s3Client=s3Client;
    this.config=config;
    this.jsonMapper=jsonMapper;
  }
  @Override public DataSegment push(  final File indexFilesDir,  DataSegment segment) throws IOException {
    log.info("Uploading [%s] to S3",indexFilesDir);
    String outputKey=JOINER.join(config.getBaseKey().isEmpty() ? null : config.getBaseKey(),segment.getDataSource(),String.format("%s_%s",segment.getInterval().getStart(),segment.getInterval().getEnd()),segment.getVersion(),segment.getShardSpec().getPartitionNum());
    long indexSize=0;
    final File zipOutFile=File.createTempFile("druid","index.zip");
    ZipOutputStream zipOut=null;
    try {
      zipOut=new ZipOutputStream(new FileOutputStream(zipOutFile));
      File[] indexFiles=indexFilesDir.listFiles();
      for (      File indexFile : indexFiles) {
        log.info("Adding indexFile[%s] with size[%,d].  Total size[%,d]",indexFile,indexFile.length(),indexSize);
        if (indexFile.length() >= Integer.MAX_VALUE) {
          throw new ISE("indexFile[%s] too large [%,d]",indexFile,indexFile.length());
        }
        zipOut.putNextEntry(new ZipEntry(indexFile.getName()));
        IOUtils.copy(new FileInputStream(indexFile),zipOut);
        indexSize+=indexFile.length();
      }
    }
  finally {
      Closeables.closeQuietly(zipOut);
    }
    try {
      S3Object toPush=new S3Object(zipOutFile);
      final String outputBucket=config.getBucket();
      toPush.setBucketName(outputBucket);
      toPush.setKey(outputKey + "/index.zip");
      log.info("Pushing %s.",toPush);
      s3Client.putObject(outputBucket,toPush);
      segment=segment.withSize(indexSize).withLoadSpec(ImmutableMap.<String,Object>of("type","s3_zip","bucket",outputBucket,"key",toPush.getKey())).withBinaryVersion(IndexIO.getVersionFromDir(indexFilesDir));
      File descriptorFile=File.createTempFile("druid","descriptor.json");
      StreamUtils.copyToFileAndClose(new ByteArrayInputStream(jsonMapper.writeValueAsBytes(segment)),descriptorFile);
      S3Object descriptorObject=new S3Object(descriptorFile);
      descriptorObject.setBucketName(outputBucket);
      descriptorObject.setKey(outputKey + "/descriptor.json");
      log.info("Pushing %s",descriptorObject);
      s3Client.putObject(outputBucket,descriptorObject);
      log.info("Deleting Index File[%s]",indexFilesDir);
      FileUtils.deleteDirectory(indexFilesDir);
      log.info("Deleting zipped index File[%s]",zipOutFile);
      zipOutFile.delete();
      log.info("Deleting descriptor file[%s]",descriptorFile);
      descriptorFile.delete();
      return segment;
    }
 catch (    NoSuchAlgorithmException e) {
      throw new IOException(e);
    }
catch (    S3ServiceException e) {
      throw new IOException(e);
    }
  }
  public S3SegmentPusher(){
  }
}
