package blobstore.s3

import blobstore.AbstractStoreTest
import blobstore.url.Authority.Bucket
import blobstore.url.{Authority, Path}
import cats.effect.IO
import cats.effect.concurrent.Ref
import cats.syntax.all._
import com.dimafeng.testcontainers.GenericContainer
import fs2.{Chunk, Stream}
import org.scalatest.{Assertion, Inside}
import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.services.s3.model.{CreateBucketRequest, StorageClass}

abstract class AbstractS3StoreTest extends AbstractStoreTest[Authority.Bucket, S3Blob] with Inside {
  def container: GenericContainer
  def client: S3AsyncClient

  override val scheme: String              = "s3"
  override val authority: Authority.Bucket = Bucket.unsafe("blobstore-test-bucket")
  override val fileSystemRoot: Path.Plain  = Path("")

  override def mkStore(): S3Store[IO] =
    new S3Store[IO](client, defaultFullMetadata = true, bufferSize = 5 * 1024 * 1024)

  def s3Store: S3Store[IO] = store.asInstanceOf[S3Store[IO]] // scalafix:ok

  override def beforeAll(): Unit = {
    container.start()
    client.createBucket(CreateBucketRequest.builder().bucket(authority.show).build()).get()
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    container.stop()
    super.afterAll()
  }

  behavior of "S3Store"

  it should "expose underlying metadata" in {
    val dir  = dirUrl("expose-underlying")
    val path = writeFile(store, dir.path)("abc.txt")

    val entities = store.list(path).compile.toList.unsafeRunSync()

    entities.foreach { s3Path =>
      inside(s3Path.representation.meta) {
        case Some(metaInfo) =>
          // Note: defaultFullMetadata = true in S3Store constructor.
          metaInfo.contentType mustBe a[Some[_]]
          metaInfo.eTag mustBe a[Some[_]]
      }
    }
  }

  def testUploadNoSize(size: Long, name: String): IO[Assertion] =
    for {
      bytes <- Stream
        .random[IO]
        .flatMap(n => Stream.chunk(Chunk.bytes(n.toString.getBytes())))
        .take(size)
        .compile
        .to(Array)
      path = Path(s"$authority/test-$testRun/multipart-upload/") / name
      url  = authority.s3 / path
      _         <- Stream.chunk(Chunk.bytes(bytes)).through(store.put(url, size = None)).compile.drain
      readBytes <- store.get(url, 4096).compile.to(Array)
      _         <- store.remove(url)
    } yield readBytes mustBe bytes

  it should "put content with no size when aligned with multi-upload boundaries 5mb" in {
    testUploadNoSize(5 * 1024 * 1024, "5mb").unsafeRunSync()
  }

  it should "put content with no size when aligned with multi-upload boundaries 15mb" in {
    testUploadNoSize(10 * 1024 * 1024, "10mb").unsafeRunSync()
  }

  it should "put content with no size when not aligned with multi-upload boundaries 7mb" in {
    testUploadNoSize(7 * 1024 * 1024, "7mb").unsafeRunSync()
  }

  it should "put content with no size when not aligned with multi-upload boundaries 12mb" in {
    testUploadNoSize(12 * 1024 * 1024, "12mb").unsafeRunSync()
  }

  it should "put rotating with file-limit > bufferSize" in {
    val dir     = dirUrl("put-rotating-s3")
    val content = randomBA(7 * 1024 * 1024)
    val data    = Stream.emits(content)

    val test = for {
      counter <- Ref.of[IO, Int](0)
      _ <- data
        .through(store.putRotate(counter.getAndUpdate(_ + 1).map(i => dir / s"$i"), 6 * 1024 * 1024))
        .compile
        .drain
      files        <- store.list(dir, recursive = true).compile.toList
      fileContents <- files.traverse(p => store.get(authority.s3 / p, 1024).compile.to(Array))
    } yield {
      files must have size 2
      files.flatMap(_.size) must contain theSameElementsAs List(6 * 1024 * 1024L, 1024 * 1024L)
      fileContents.flatten mustBe content.toList
    }

    test.unsafeRunSync()
  }

  it should "resolve type of storage class" in {
    store.list(dirUrl("foo")).map { path =>
      val sc: Option[StorageClass] = path.storageClass
      sc mustBe None
    }
  }
}
