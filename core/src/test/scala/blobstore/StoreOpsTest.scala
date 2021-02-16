package blobstore

import java.nio.charset.{Charset, StandardCharsets}
import java.nio.file.{Files => JFiles}
import blobstore.url.{Authority, Path, Url}
import blobstore.url.Authority.Bucket
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.MonadError
import fs2.io.file.Files
import fs2.{Pipe, Stream}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers

import scala.collection.mutable.ArrayBuffer

class StoreOpsTest extends AnyFlatSpec with Matchers {

  behavior of "PutOps"
  it should "buffer contents and compute size before calling Store.put" in {
    val bytes: Array[Byte] = "AAAAAAAAAA".getBytes(Charset.forName("utf-8"))
    val store              = DummyStore()

    Stream
      .emits(bytes)
      .covary[IO]
      .through(store.bufferedPut(
        Url.unsafe[Bucket]("foo://bucket/path/to/file.txt"),
        overwrite = true,
        chunkSize = 4096
      ))
      .compile
      .drain
      .unsafeRunSync()

    store.buf.toArray must be(bytes)
  }

  it should "upload a file from a nio Path" in {
    val bytes = "hello".getBytes(Charset.forName("utf-8"))
    val store = DummyStore()
    Stream.resource(Files[IO].tempFile(prefix = "test-file", suffix = ".bin")).flatMap { p =>
      Stream.emits(bytes).covary[IO].through(Files[IO].writeAll(p)).drain ++
        Stream.eval(store.put(p, Url.unsafe[Bucket]("foo://bucket/path/to/file.txt"), true))
    }.compile
      .drain
      .unsafeRunSync()
    store.buf.toArray must be(bytes)
  }

  it should "download a file to a nio path" in {
    val bytes = "hello".getBytes(Charset.forName("utf-8"))
    val store = DummyStore()
    val path  = Url.unsafe[Bucket]("foo://bucket/path/to/file.txt")
    Stream.emits(bytes).through(store.put(path)).compile.drain.unsafeRunSync()

    Stream
      .bracket(IO(JFiles.createTempFile("test-file", ".bin")))(p => IO(p.toFile.delete).void)
      .flatMap { nioPath =>
        Stream.eval(store.get(path, nioPath, 4096)) >> Stream.eval {
          IO {
            JFiles.readAllBytes(nioPath) mustBe bytes
          }
        }
      }
      .compile
      .drain
      .unsafeRunSync()
  }

  it should "transferTo" in {
    val u1   = Url.unsafe[Bucket]("gs://foo")
    val from = DummyStore.withContents("foo")
    val to   = DummyStore()

    from.transferTo(to, u1, u1).unsafeRunSync()

    val result = new String(to.buf.toArray, StandardCharsets.UTF_8)

    result mustBe "foo"
  }
}

final case class DummyStore() extends Store[IO, Authority.Bucket, String] {
  val buf = new ArrayBuffer[Byte]()
  override def put(url: Url[Bucket], overwrite: Boolean, size: Option[Long] = None): Pipe[IO, Byte, Unit] = {
    in =>
      {
        buf.appendAll(in.compile.toVector.unsafeRunSync())
        Stream.emit(())
      }
  }
  override def get(url: Url[Authority.Bucket], chunkSize: Int): Stream[IO, Byte] = Stream.emits(buf)
  override def list(url: Url[Authority.Bucket], recursive: Boolean = false): Stream[IO, Path.Plain] =
    Stream.emits(List(Path("the-file.txt")))
  override def move(src: Url[Bucket], dst: Url[Bucket]): IO[Unit]                         = ???
  override def copy(src: Url[Bucket], dst: Url[Bucket]): IO[Unit]                         = ???
  override def remove(url: Url[Bucket], recursive: Boolean): IO[Unit]                     = ???
  override def putRotate(computePath: IO[Url[Bucket]], limit: Long): Pipe[IO, Byte, Unit] = ???

  override def stat(url: Url[Bucket]): Stream[IO, Path[String]] = ???

  override def widen(implicit ME: MonadError[IO, Throwable]): Store[IO, Authority.Standard, String] = ???
}

object DummyStore {
  def withContents(s: String): DummyStore = {
    val store = DummyStore()
    store.buf.appendAll(s.getBytes(StandardCharsets.UTF_8))
    store
  }
}
